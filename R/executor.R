# Module: Step Execution
# Called by worker daemon or inline for session-only jobs.

.BLOCKED_ENV_VARS <- c("PATH", "HOME", "USER", "SHELL",
  "LD_PRELOAD", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH",
  "DYLD_INSERT_LIBRARIES", "PYTHONPATH", "PYTHONSTARTUP",
  "BASH_ENV", "ENV", "CDPATH", "IFS")

# Output kinds that are safe to return to the client via jobResultDS.
# Everything else stays server-side (loadable via jobLoadOutputDS).
.CLIENT_SAFE_KINDS <- c("summary", "aggregate_result", "job_metadata")

#' Execute the current step of a job
#' @keywords internal
.executor_run_step <- function(db, job_id, step_index, spec) {
  step <- spec$steps[[step_index]]
  step_dir <- .ensure_step_dir(job_id, step_index)
  input_dir <- .resolve_step_input(db, job_id, step_index, step)

  .store_update_step(db, job_id, step_index, state = "running",
    started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  if (identical(step$plane, "session")) {
    .run_session_step(db, job_id, step_index, step, step_dir, input_dir)
  } else {
    .run_artifact_step(db, job_id, step_index, step, step_dir, input_dir)
  }
}

#' Advance after step success
#' @keywords internal
.executor_advance <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (is.null(job)) return()
  current <- as.integer(job$step_index)
  total <- as.integer(job$total_steps)

  if (current >= total) {
    .build_job_result(db, job_id)
    .store_update_job(db, job_id, state = "FINISHED", worker_pid = NA_integer_,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "finished")
    return()
  }

  next_idx <- current + 1L
  .store_update_job(db, job_id, step_index = next_idx)
  spec <- .store_get_spec(db, job_id)
  .executor_run_step(db, job_id, next_idx, spec)
}

#' Kill a worker process
#' @keywords internal
.executor_kill <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (!is.null(job) && !is.na(job$worker_pid)) {
    pid <- as.integer(job$worker_pid)
    if (.pid_is_alive(pid)) {
      tools::pskill(pid, signal = 15L)
      Sys.sleep(2)
      if (.pid_is_alive(pid)) tools::pskill(pid, signal = 9L)
    }
    .store_update_job(db, job_id, worker_pid = NA_integer_)
  }
}

#' Build the final result object for a completed job
#'
#' DISCLOSURE RULE: Only outputs of kind "summary", "aggregate_result",
#' or "job_metadata" can have their values returned to the client via
#' jobResultDS(). All other outputs (emit_value, artifact_file, etc.)
#' are listed by name/kind only -- their values stay server-side and
#' must be loaded via jobLoadOutputDS() (assign) or published as assets.
#'
#' @keywords internal
.build_job_result <- function(db, job_id) {
  home <- .dsjobs_home()
  result_dir <- file.path(home, "artifacts", job_id, "result")
  dir.create(result_dir, recursive = TRUE, showWarnings = FALSE)

  safe_result <- list(
    job_id = job_id,
    ready = TRUE
  )

  # Only summary/aggregate_result outputs cross the wire with values
  safe_outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref, size_bytes FROM outputs
     WHERE job_id = ? AND kind IN ('summary', 'aggregate_result', 'job_metadata')",
    params = list(job_id))

  if (nrow(safe_outputs) > 0) {
    safe_result$summaries <- lapply(seq_len(nrow(safe_outputs)), function(i) {
      row <- safe_outputs[i, ]
      out <- list(name = row$name, kind = row$kind)
      if (!is.na(row$path_or_ref) && file.exists(row$path_or_ref) &&
          grepl("\\.rds$", row$path_or_ref)) {
        out$value <- readRDS(row$path_or_ref)
      }
      out
    })
  }

  # ALL outputs listed by name/kind only (no values) -- for discoverability
  all_outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, size_bytes FROM outputs WHERE job_id = ?",
    params = list(job_id))

  safe_result$available_outputs <- if (nrow(all_outputs) > 0) {
    lapply(seq_len(nrow(all_outputs)), function(i) {
      list(name = all_outputs$name[i], kind = all_outputs$kind[i],
           size_bytes = all_outputs$size_bytes[i])
    })
  } else list()

  saveRDS(safe_result, file.path(result_dir, "result.rds"))
  safe_result
}

# --- Helpers ---

#' @keywords internal
.ensure_step_dir <- function(job_id, step_index) {
  home <- .dsjobs_home()
  step_dir <- file.path(home, "artifacts", job_id,
                         sprintf("step_%03d", step_index))
  dir.create(file.path(step_dir, "output"), recursive = TRUE, showWarnings = FALSE)
  Sys.chmod(step_dir, "0700")
  step_dir
}

#' @keywords internal
.resolve_step_input <- function(db, job_id, step_index, step_spec) {
  if (!is.null(step_spec$inputs)) {
    refs <- step_spec$inputs
    if (is.list(refs) && length(refs) > 0) {
      ref <- refs[[1]]
      if (is.numeric(ref)) {
        row <- DBI::dbGetQuery(db,
          "SELECT output_ref FROM steps WHERE job_id = ? AND step_index = ?",
          params = list(job_id, as.integer(ref)))
        if (nrow(row) > 0 && !is.na(row$output_ref[1]))
          return(file.path(.dsjobs_home(), row$output_ref[1]))
      }
    }
  }
  if (step_index > 1L) {
    row <- DBI::dbGetQuery(db,
      "SELECT output_ref FROM steps WHERE job_id = ? AND step_index = ?",
      params = list(job_id, step_index - 1L))
    if (nrow(row) > 0 && !is.na(row$output_ref[1]))
      return(file.path(.dsjobs_home(), row$output_ref[1]))
  }
  NULL
}
