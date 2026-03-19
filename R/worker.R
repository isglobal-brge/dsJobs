# Module: Worker Daemon
# External process. NOT started from .onAttach().
# Started by admin via: Rscript inst/worker/main.R /var/lib/dsjobs
# Or by helper: dsJobs:::.dsjobs_worker_start()
# Supervised by systemd, Docker restart policy, or cron.

#' Start the worker daemon (admin/setup helper, NOT auto-start)
#' @keywords internal
.dsjobs_worker_start <- function() {
  home <- .dsjobs_home()
  pid_file <- file.path(home, "worker.pid")
  if (file.exists(pid_file)) {
    pid <- tryCatch(as.integer(readLines(pid_file, n = 1, warn = FALSE)),
                     error = function(e) NA_integer_)
    if (.pid_is_alive(pid)) {
      message("dsJobs worker already running (PID ", pid, ")")
      return(invisible(NULL))
    }
    unlink(pid_file)
  }
  log_file <- file.path(home, "worker.log")
  worker_script <- system.file("worker", "main.R", package = "dsJobs")
  proc <- processx::process$new(
    command = file.path(R.home("bin"), "Rscript"),
    args = c(worker_script, home),
    stdout = log_file, stderr = log_file,
    cleanup = FALSE, cleanup_tree = FALSE)
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
    pid <- tryCatch(as.integer(readLines(pid_file, n = 1, warn = FALSE)),
                     error = function(e) NA_integer_)
    if (.pid_is_alive(pid)) {
      tools::pskill(pid, signal = 15L); Sys.sleep(2)
      if (.pid_is_alive(pid)) tools::pskill(pid, signal = 9L)
    }
    unlink(pid_file)
  }
  message("dsJobs worker stopped.")
}

#' Write worker health file (for monitoring)
#' @keywords internal
.worker_write_health <- function() {
  home <- .dsjobs_home()
  health <- list(
    pid = Sys.getpid(),
    alive = TRUE,
    last_heartbeat = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
    uptime_secs = as.numeric(difftime(Sys.time(),
      .dsjobs_env$.worker_started_at %||% Sys.time(), units = "secs"))
  )
  health_path <- file.path(home, "worker.health")
  writeLines(jsonlite::toJSON(health, auto_unbox = TRUE, pretty = TRUE), health_path)
}

#' Check worker health status
#' @keywords internal
.dsjobs_worker_health <- function() {
  home <- .dsjobs_home(must_exist = FALSE)
  if (is.null(home)) return(list(alive = FALSE, reason = "no DSJOBS_HOME"))
  health_path <- file.path(home, "worker.health")
  if (!file.exists(health_path)) return(list(alive = FALSE, reason = "no health file"))
  tryCatch({
    h <- jsonlite::fromJSON(readLines(health_path, warn = FALSE))
    last <- as.POSIXct(h$last_heartbeat, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
    stale <- as.numeric(difftime(Sys.time(), last, units = "secs")) > 30
    list(alive = !stale && .pid_is_alive(h$pid), pid = h$pid,
         last_heartbeat = h$last_heartbeat, stale = stale)
  }, error = function(e) list(alive = FALSE, reason = e$message))
}

#' Main worker loop (runs inside the worker process)
#' @keywords internal
.worker_main <- function() {
  db <- .db_connect()
  on.exit(.db_close(db))
  settings <- .dsjobs_settings()
  gc_counter <- 0L
  inbox_counter <- 0L
  .dsjobs_env$.worker_started_at <- Sys.time()
  .worker_log("Worker started (PID ", Sys.getpid(), ")")

  # Connect to control plane backend (service credential)
  cp_conn <- tryCatch(.worker_cp_connect(), error = function(e) {
    .worker_log("No control plane backend: ", conditionMessage(e))
    NULL
  })
  if (!is.null(cp_conn)) .worker_log("Control plane connected")
  on.exit(if (!is.null(cp_conn)) .worker_cp_disconnect(cp_conn), add = TRUE)

  repeat {
    tryCatch({
      .worker_write_health()

      # Scan inboxes periodically (every 5 cycles)
      inbox_counter <- inbox_counter + 1L
      if (!is.null(cp_conn) && inbox_counter >= 5L) {
        .worker_import_inboxes(db, cp_conn)
        inbox_counter <- 0L
      }

      .worker_reap(db, cp_conn)
      .worker_dispatch(db, cp_conn)

      gc_counter <- gc_counter + 1L
      if (gc_counter >= 100L) { .worker_gc(db); gc_counter <- 0L }
    }, error = function(e) .worker_log("ERROR: ", conditionMessage(e)))
    Sys.sleep(settings$worker_poll_secs)
  }
}

#' Import new submissions from inboxes into SQLite
#' @keywords internal
.worker_import_inboxes <- function(db, cp_conn) {
  submissions <- .worker_scan_inboxes(cp_conn)
  if (length(submissions) == 0) return()

  for (sub in submissions) {
    spec <- sub$spec
    job_id <- spec$job_id
    if (is.null(job_id)) next

    # Check if already imported
    existing <- .store_get_job(db, job_id)
    if (!is.null(existing)) {
      # Already in SQLite -- remove from inbox
      .worker_consume_inbox(cp_conn, sub$inbox_file)
      next
    }

    # Import to SQLite
    owner_id <- sub$owner
    token_hash <- spec$.access_token_hash
    spec_for_hash <- spec[setdiff(names(spec), c("job_id", ".owner", ".access_token_hash"))]
    spec_hash <- if (identical(spec$visibility, "global"))
      digest::digest(jsonlite::toJSON(spec_for_hash, auto_unbox = TRUE),
        algo = "sha256", serialize = FALSE) else NULL

    tryCatch({
      .store_create_job(db, job_id, owner_id, spec, length(spec$steps),
        access_token_hash = token_hash, spec_hash = spec_hash)
      .worker_log("Imported job ", job_id, " from ", owner_id)
      .worker_consume_inbox(cp_conn, sub$inbox_file)
    }, error = function(e)
      .worker_log("Import failed for ", job_id, ": ", conditionMessage(e)))
  }
}

#' @keywords internal
.worker_dispatch <- function(db, cp_conn = NULL) {
  settings <- .dsjobs_settings()
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    running_n <- DBI::dbGetQuery(db,
      "SELECT COUNT(*) AS n FROM jobs WHERE state = 'RUNNING'")$n
    slots <- settings$max_jobs_global - running_n
    if (slots <= 0) { DBI::dbExecute(db, "COMMIT"); return() }

    pending <- DBI::dbGetQuery(db,
      "SELECT job_id FROM jobs WHERE state = 'PENDING' ORDER BY submitted_at LIMIT ?",
      params = list(slots))
    for (jid in pending$job_id) {
      tryCatch({
        spec <- .store_get_spec(db, jid)
        if (is.null(spec)) next
        .store_update_job(db, jid, state = "RUNNING", step_index = 1L,
          started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
        .db_log_event(db, jid, "started")
        .executor_run_step(db, jid, 1L, spec)
        # Write mirror after execution
        .worker_sync_mirror(db, cp_conn, jid)
      }, error = function(e) {
        .store_update_job(db, jid, state = "FAILED",
          error_message = paste("Dispatch failed:", conditionMessage(e)),
          finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
        .worker_sync_mirror(db, cp_conn, jid)
      })
    }
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    .worker_log("Dispatch error: ", conditionMessage(e))
  })
}

#' @keywords internal
#' Sync job state to Opal mirror
#' @keywords internal
.worker_sync_mirror <- function(db, cp_conn, job_id) {
  if (is.null(cp_conn)) return()
  job <- .store_get_job(db, job_id)
  if (is.null(job)) return()

  state_obj <- list(
    job_id = job$job_id, state = job$state,
    step_index = as.integer(job$step_index),
    total_steps = as.integer(job$total_steps),
    label = job$label, tags = job$tags,
    visibility = job$visibility, owner_id = job$owner_id,
    submitted_at = job$submitted_at, started_at = job$started_at,
    finished_at = job$finished_at, error = job$error_message
  )
  .worker_write_mirror(cp_conn, job$owner_id, job_id, state_obj)

  # If finished, also write result mirror
  if (job$state %in% c("FINISHED", "PUBLISHED")) {
    home <- .dsjobs_home()
    result_path <- file.path(home, "artifacts", job_id, "result", "result.rds")
    if (file.exists(result_path)) {
      result <- readRDS(result_path)
      .worker_write_result_mirror(cp_conn, job$owner_id, job_id, result)
    }
  }
}

#' @keywords internal
.worker_reap <- function(db, cp_conn = NULL) {
  running <- DBI::dbGetQuery(db,
    "SELECT job_id, worker_pid, step_index FROM jobs
     WHERE state = 'RUNNING' AND worker_pid IS NOT NULL")
  if (nrow(running) == 0) return()

  for (i in seq_len(nrow(running))) {
    pid <- as.integer(running$worker_pid[i])
    jid <- running$job_id[i]
    sidx <- as.integer(running$step_index[i])

    if (!.pid_is_alive(pid)) {
      step_dir <- file.path(.dsjobs_home(), "artifacts", jid,
                             sprintf("step_%03d", sidx))
      exit_code <- .read_exit_code(step_dir)

      DBI::dbExecute(db, "BEGIN IMMEDIATE")
      tryCatch({
        if (identical(exit_code, 0L)) {
          output_ref <- file.path("artifacts", jid,
                                   sprintf("step_%03d", sidx), "output")
          # Register artifact outputs
          out_dir <- file.path(.dsjobs_home(), output_ref)
          if (dir.exists(out_dir)) {
            files <- list.files(out_dir, full.names = TRUE)
            for (f in files) {
              .db_register_output(db, jid, sidx, basename(f),
                "artifact_file", f, file.info(f)$size, safe_for_client = FALSE)
            }
          }
          .store_update_step(db, jid, sidx, state = "done", exit_code = 0L,
            output_ref = output_ref,
            finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
          .store_update_job(db, jid, worker_pid = NA_integer_)
          .db_log_event(db, jid, "step_done", list(step_index = sidx))
          .executor_advance(db, jid)
        } else {
          settings <- .dsjobs_settings()
          job <- .store_get_job(db, jid)
          retries <- as.integer(job$retry_count %||% 0L)
          .store_update_step(db, jid, sidx, state = "failed",
            exit_code = exit_code, error_message = paste("Exit:", exit_code),
            finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
          if (retries < settings$max_retries) {
            .store_update_job(db, jid, state = "PENDING",
              retry_count = retries + 1L, worker_pid = NA_integer_)
          } else {
            .store_update_job(db, jid, state = "FAILED",
              error_message = paste("Step", sidx, "failed (exit", exit_code, ")"),
              worker_pid = NA_integer_,
              finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
          }
        }
        DBI::dbExecute(db, "COMMIT")
        .worker_sync_mirror(db, cp_conn, jid)
      }, error = function(e) {
        tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
      })
    }
  }
}

#' @keywords internal
.read_exit_code <- function(step_dir) {
  ef <- file.path(step_dir, "exit_code")
  if (file.exists(ef)) {
    code <- tryCatch(as.integer(readLines(ef, n = 1, warn = FALSE)),
                      error = function(e) NA_integer_)
    if (!is.na(code)) return(code)
  }
  stderr_path <- file.path(step_dir, "stderr.log")
  if (file.exists(stderr_path) && file.info(stderr_path)$size > 0) return(1L)
  0L
}

#' @keywords internal
.worker_gc <- function(db) {
  settings <- .dsjobs_settings()
  cutoff <- format(Sys.time() - settings$job_expiry_hours * 3600,
                    "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  expired <- DBI::dbGetQuery(db,
    "SELECT job_id FROM jobs
     WHERE state IN ('FINISHED','PUBLISHED','FAILED','CANCELLED')
       AND finished_at IS NOT NULL AND finished_at < ?",
    params = list(cutoff))
  for (jid in expired$job_id) {
    DBI::dbExecute(db, "DELETE FROM outputs WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM events WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM steps WHERE job_id = ?", params = list(jid))
    DBI::dbExecute(db, "DELETE FROM jobs WHERE job_id = ?", params = list(jid))
    ad <- file.path(.dsjobs_home(), "artifacts", jid)
    if (dir.exists(ad)) unlink(ad, recursive = TRUE)
  }
  if (nrow(expired) > 0) .worker_log("GC removed ", nrow(expired), " jobs")
}

#' @keywords internal
.worker_log <- function(...) {
  message("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", paste0(...))
}
