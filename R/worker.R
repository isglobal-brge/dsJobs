# Module: Worker Daemon
# Separate R process. Polls DB, dispatches, reaps, GCs.

#' Start the worker daemon
#' @keywords internal
.dsjobs_worker_start <- function() {
  home <- .dsjobs_home()
  pid_file <- file.path(home, "worker.pid")

  # Check if already running
  if (file.exists(pid_file)) {
    existing_pid <- as.integer(readLines(pid_file, n = 1, warn = FALSE))
    if (.pid_is_alive(existing_pid)) {
      message("dsJobs worker already running (PID ", existing_pid, ")")
      return(invisible(NULL))
    }
    unlink(pid_file)
  }

  log_file <- file.path(home, "worker.log")
  worker_script <- system.file("worker", "main.R", package = "dsJobs")

  proc <- processx::process$new(
    command = file.path(R.home("bin"), "Rscript"),
    args = c(worker_script, home),
    stdout = log_file,
    stderr = log_file,
    cleanup = FALSE,
    cleanup_tree = FALSE
  )

  writeLines(as.character(proc$get_pid()), pid_file)
  .dsjobs_env$.worker <- proc
  message("dsJobs worker started (PID ", proc$get_pid(), ")")
  invisible(proc$get_pid())
}

#' Stop the worker daemon
#' @keywords internal
.dsjobs_worker_stop <- function() {
  home <- .dsjobs_home()
  pid_file <- file.path(home, "worker.pid")

  if (file.exists(pid_file)) {
    pid <- as.integer(readLines(pid_file, n = 1, warn = FALSE))
    if (.pid_is_alive(pid)) {
      tools::pskill(pid, signal = 15L)
      Sys.sleep(2)
      if (.pid_is_alive(pid)) {
        tools::pskill(pid, signal = 9L)
      }
    }
    unlink(pid_file)
  }

  # Also stop tracked process
  proc <- tryCatch(get(".worker", envir = .dsjobs_env), error = function(e) NULL)
  if (!is.null(proc) && inherits(proc, "process") && proc$is_alive()) {
    proc$signal(15L)
    proc$wait(timeout = 5000)
    if (proc$is_alive()) proc$kill()
  }
  message("dsJobs worker stopped.")
}

#' Main worker loop (runs in the worker process)
#' @keywords internal
.worker_main <- function() {
  db <- .db_connect()
  on.exit(.db_close(db))

  settings <- .dsjobs_settings()
  gc_counter <- 0L

  .worker_log("Worker started (PID ", Sys.getpid(), ")")

  repeat {
    tryCatch({
      # 1. Reap finished artifact workers
      .worker_reap(db)

      # 2. Dispatch pending jobs (inside transaction)
      .worker_dispatch(db)

      # 3. Periodic GC (every 100 iterations)
      gc_counter <- gc_counter + 1L
      if (gc_counter >= 100L) {
        .worker_gc(db)
        gc_counter <- 0L
      }
    }, error = function(e) {
      .worker_log("ERROR: ", conditionMessage(e))
    })

    Sys.sleep(settings$worker_poll_secs)
  }
}

#' Dispatch pending jobs
#' @keywords internal
.worker_dispatch <- function(db) {
  settings <- .dsjobs_settings()

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    # Count running jobs
    running <- DBI::dbGetQuery(db,
      "SELECT COUNT(*) AS n FROM jobs WHERE state = 'RUNNING'")$n

    slots <- settings$max_jobs_global - running
    if (slots <= 0) {
      DBI::dbExecute(db, "COMMIT")
      return()
    }

    # Pick pending jobs FIFO
    pending <- DBI::dbGetQuery(db,
      "SELECT job_id FROM jobs WHERE state = 'PENDING'
       ORDER BY submitted_at LIMIT ?",
      params = list(slots))

    for (jid in pending$job_id) {
      tryCatch({
        spec <- .store_get_spec(db, jid)
        if (is.null(spec)) next

        # Transition to RUNNING
        .store_update_job(db, jid,
          state = "RUNNING",
          step_index = 1L,
          started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
        .db_log_event(db, jid, "started", list(step_index = 1L))

        # Execute first step
        .executor_run_step(db, jid, 1L, spec)
      }, error = function(e) {
        .store_update_job(db, jid,
          state = "FAILED",
          error = paste("Dispatch failed:", conditionMessage(e)),
          finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
        .db_log_event(db, jid, "dispatch_failed", list(error = conditionMessage(e)))
      })
    }

    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    .worker_log("Dispatch error: ", conditionMessage(e))
  })
}

#' Reap finished artifact workers
#' @keywords internal
.worker_reap <- function(db) {
  running <- DBI::dbGetQuery(db,
    "SELECT job_id, worker_pid, step_index FROM jobs
     WHERE state = 'RUNNING' AND worker_pid IS NOT NULL")

  if (nrow(running) == 0) return()

  for (i in seq_len(nrow(running))) {
    pid <- as.integer(running$worker_pid[i])
    jid <- running$job_id[i]
    step_idx <- as.integer(running$step_index[i])

    if (!.pid_is_alive(pid)) {
      # Worker died -- check exit code
      step_dir <- file.path(.dsjobs_home(), "artifacts", jid,
                             sprintf("step_%03d", step_idx))
      exit_code <- .read_exit_code(step_dir)

      DBI::dbExecute(db, "BEGIN IMMEDIATE")
      tryCatch({
        if (identical(exit_code, 0L)) {
          output_ref <- file.path("artifacts", jid,
                                   sprintf("step_%03d", step_idx), "output")
          .store_update_step(db, jid, step_idx,
            state = "done", exit_code = 0L, output_ref = output_ref,
            finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
          .store_update_job(db, jid, worker_pid = NA_integer_)
          .db_log_event(db, jid, "step_done",
            list(step_index = step_idx, exit_code = 0L))

          # Advance to next step
          .executor_advance(db, jid)
        } else {
          # Step failed -- retry or fail job
          settings <- .dsjobs_settings()
          job <- .store_get_job(db, jid)
          retries <- as.integer(job$retries %||% 0L)

          .store_update_step(db, jid, step_idx,
            state = "failed", exit_code = exit_code,
            error = paste("Exit code:", exit_code),
            finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

          if (retries < settings$max_retries) {
            .store_update_job(db, jid,
              state = "PENDING",
              retries = retries + 1L,
              worker_pid = NA_integer_)
            .db_log_event(db, jid, "retry",
              list(step_index = step_idx, attempt = retries + 1L))
          } else {
            .store_update_job(db, jid,
              state = "FAILED",
              error = paste("Step", step_idx, "failed (exit", exit_code, ")"),
              worker_pid = NA_integer_,
              finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
            .db_log_event(db, jid, "failed",
              list(step_index = step_idx, exit_code = exit_code))
          }
        }
        DBI::dbExecute(db, "COMMIT")
      }, error = function(e) {
        tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
        .worker_log("Reap error for ", jid, ": ", conditionMessage(e))
      })
    }

    # Check for timeout
    .worker_check_timeout(db, jid, pid, step_idx)
  }
}

#' Check if a running artifact worker has exceeded its timeout
#' @keywords internal
.worker_check_timeout <- function(db, job_id, pid, step_index) {
  if (!.pid_is_alive(pid)) return()

  step_row <- DBI::dbGetQuery(db,
    "SELECT started_at FROM steps WHERE job_id = ? AND step_index = ?",
    params = list(job_id, step_index))

  if (nrow(step_row) == 0 || is.na(step_row$started_at[1])) return()

  started <- as.POSIXct(step_row$started_at[1],
                          format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
  elapsed <- as.numeric(difftime(Sys.time(), started, units = "secs"))
  timeout <- as.numeric(.dsjobs_settings()$default_timeout_secs)

  if (elapsed > timeout) {
    tools::pskill(pid, signal = 9L)
    .worker_log("Killed timed-out worker for ", job_id, " (", round(elapsed), "s)")
  }
}

#' Read exit code from step directory (heuristic)
#' @keywords internal
.read_exit_code <- function(step_dir) {
  # Check for explicit exit code file
  exit_file <- file.path(step_dir, "exit_code")
  if (file.exists(exit_file)) {
    code <- tryCatch(as.integer(readLines(exit_file, n = 1, warn = FALSE)),
                      error = function(e) NA_integer_)
    if (!is.na(code)) return(code)
  }
  # Check stderr for error indicators
  stderr_path <- file.path(step_dir, "stderr.log")
  if (file.exists(stderr_path)) {
    stderr_size <- file.info(stderr_path)$size
    if (!is.na(stderr_size) && stderr_size > 0) return(1L)
  }
  # Assume success if no evidence of failure
  0L
}

#' Run periodic garbage collection
#' @keywords internal
.worker_gc <- function(db) {
  settings <- .dsjobs_settings()
  expiry_hours <- settings$job_expiry_hours
  cutoff <- format(Sys.time() - expiry_hours * 3600,
                    "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  # Find expired terminal jobs
  expired <- DBI::dbGetQuery(db,
    "SELECT job_id FROM jobs
     WHERE state IN ('FINISHED', 'PUBLISHED', 'FAILED', 'CANCELLED')
       AND finished_at IS NOT NULL AND finished_at < ?",
    params = list(cutoff))

  for (jid in expired$job_id) {
    # Delete from DB
    DBI::dbExecute(db, "DELETE FROM events WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM steps WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM jobs WHERE job_id = ?", params = list(jid))

    # Delete artifacts
    artifact_dir <- file.path(.dsjobs_home(), "artifacts", jid)
    if (dir.exists(artifact_dir)) {
      unlink(artifact_dir, recursive = TRUE)
    }
  }

  if (nrow(expired) > 0) {
    .worker_log("GC removed ", nrow(expired), " expired jobs")
  }
}

#' Write to worker log
#' @keywords internal
.worker_log <- function(...) {
  msg <- paste0("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", paste0(...))
  message(msg)
}
