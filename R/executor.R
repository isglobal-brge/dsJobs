# Module: Step Execution
# Dispatches steps, manages advancement, kills workers.
# Called by the worker daemon, NOT by DataSHIELD methods.

#' Execute the current step of a job
#'
#' Called by the worker inside a transaction.
#'
#' @param db DBI connection.
#' @param job_id Character.
#' @param step_index Integer (1-based).
#' @param spec Named list; full job spec.
#' @return Invisible NULL.
#' @keywords internal
.executor_run_step <- function(db, job_id, step_index, spec) {
  step <- spec$steps[[step_index]]
  step_dir <- .ensure_step_dir(job_id, step_index)

  # Resolve input from previous step
  input_dir <- .resolve_step_input(db, job_id, step_index, step)

  .store_update_step(db, job_id, step_index,
    state = "running",
    started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  if (identical(step$plane, "session")) {
    .run_session_step(db, job_id, step_index, step, step_dir, input_dir)
  } else {
    .run_artifact_step(db, job_id, step_index, step, step_dir, input_dir)
  }
}

#' Advance a job after a step completes successfully
#' @keywords internal
.executor_advance <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (is.null(job)) return()

  spec <- .store_get_spec(db, job_id)
  current <- as.integer(job$step_index)
  total <- as.integer(job$total_steps)

  if (current >= total) {
    # All steps done
    .store_update_job(db, job_id,
      state = "FINISHED",
      worker_pid = NA_integer_,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "finished", list(total_steps = total))

    # Auto-publish if configured
    if (!is.null(spec$publish)) {
      tryCatch({
        .publish_safe_result(job_id, spec, db)
        .store_update_job(db, job_id, state = "PUBLISHED")
        .db_log_event(db, job_id, "published")
      }, error = function(e) {
        .db_log_event(db, job_id, "publish_failed", list(error = e$message))
      })
    }
    return()
  }

  # Start next step
  next_idx <- current + 1L
  .store_update_job(db, job_id, step_index = next_idx)
  .executor_run_step(db, job_id, next_idx, spec)
}

#' Kill a job's active worker process
#' @keywords internal
.executor_kill <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (!is.null(job) && !is.na(job$worker_pid)) {
    pid <- as.integer(job$worker_pid)
    if (.pid_is_alive(pid)) {
      tools::pskill(pid, signal = 15L)  # SIGTERM
      Sys.sleep(2)
      if (.pid_is_alive(pid)) {
        tools::pskill(pid, signal = 9L)  # SIGKILL
      }
    }
    .store_update_job(db, job_id, worker_pid = NA_integer_)
  }
}

#' Ensure step artifact directory exists
#' @keywords internal
.ensure_step_dir <- function(job_id, step_index) {
  home <- .dsjobs_home()
  step_dir <- file.path(home, "artifacts", job_id,
                         sprintf("step_%03d", step_index))
  dir.create(file.path(step_dir, "output"), recursive = TRUE, showWarnings = FALSE)
  Sys.chmod(step_dir, "0700")
  step_dir
}

#' Resolve step input directory from previous step or explicit ref
#' @keywords internal
.resolve_step_input <- function(db, job_id, step_index, step_spec) {
  # Explicit input_from in spec
  if (!is.null(step_spec$input_from)) {
    ref_step <- as.integer(step_spec$input_from)
    row <- DBI::dbGetQuery(db,
      "SELECT output_ref FROM steps WHERE job_id = ? AND step_index = ?",
      params = list(job_id, ref_step))
    if (nrow(row) > 0 && !is.na(row$output_ref[1])) {
      return(file.path(.dsjobs_home(), row$output_ref[1]))
    }
  }
  # Default: previous step's output
  if (step_index > 1L) {
    row <- DBI::dbGetQuery(db,
      "SELECT output_ref FROM steps WHERE job_id = ? AND step_index = ?",
      params = list(job_id, step_index - 1L))
    if (nrow(row) > 0 && !is.na(row$output_ref[1])) {
      return(file.path(.dsjobs_home(), row$output_ref[1]))
    }
  }
  NULL
}
