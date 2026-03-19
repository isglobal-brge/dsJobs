# Module: Resource-Class Scheduling
# Lazy/triggered scheduler -- no background daemon. Dispatches jobs on
# submit and status poll, fitting Rock's session model.

#' Dispatch pending jobs
#'
#' Called during jobSubmitDS and jobStatusDS. Picks up PENDING jobs and
#' starts their execution if resources are available.
#'
#' @return Invisible NULL.
#' @keywords internal
.scheduler_dispatch <- function() {
  # First reap any finished workers
  .scheduler_reap()

  settings <- .dsjobs_settings()

  # Count currently RUNNING jobs

  running <- .store_list_jobs(states = "RUNNING")
  n_running <- nrow(running)

  if (n_running >= settings$max_concurrent_jobs) {
    return(invisible(NULL))
  }

  # Pick up PENDING jobs (FIFO by submission time)
  pending <- .store_list_jobs(states = "PENDING")
  if (nrow(pending) == 0) return(invisible(NULL))

  # Sort by submitted_at
  pending <- pending[order(pending$submitted_at), , drop = FALSE]

  # Start as many as quota allows
  slots <- settings$max_concurrent_jobs - n_running
  to_start <- utils::head(pending$job_id, slots)

  for (job_id in to_start) {
    tryCatch(
      .executor_start(job_id),
      error = function(e) {
        .store_update_state(job_id, list(
          state = "FAILED",
          error = paste("Dispatch failed:", conditionMessage(e)),
          finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
        ))
        .audit_log(job_id, "dispatch_failed", list(error = conditionMessage(e)))
      }
    )
  }

  invisible(NULL)
}

#' Reap finished workers
#'
#' Checks all RUNNING jobs for dead processx workers and advances or
#' fails them accordingly.
#'
#' @return Invisible NULL.
#' @keywords internal
.scheduler_reap <- function() {
  running <- .store_list_jobs(states = "RUNNING")
  if (nrow(running) == 0) return(invisible(NULL))

  for (jid in running$job_id) {
    worker_key <- paste0("worker_", jid)
    proc <- tryCatch(
      get(worker_key, envir = .dsjobs_env),
      error = function(e) NULL
    )

    if (is.null(proc)) {
      # No tracked worker -- check if we should advance or fail
      state <- .store_read_state(jid)
      if (!is.null(state)) {
        spec <- .store_read_spec(jid)
        if (!is.null(spec)) {
          .executor_advance(jid, state, spec)
        }
      }
      next
    }

    if (inherits(proc, "process") && !proc$is_alive()) {
      exit_code <- proc$get_exit_status()
      state <- .store_read_state(jid)
      spec <- .store_read_spec(jid)

      if (exit_code == 0L) {
        # Step succeeded -- record and advance
        step_idx <- as.integer(state$step_index)
        .audit_log(jid, "step_done", list(
          step_index = step_idx, exit_code = exit_code
        ))
        .executor_advance(jid, state, spec)
      } else {
        # Step failed
        settings <- .dsjobs_settings()
        retries <- as.integer(state$retries %||% 0L)
        if (retries < settings$max_retries) {
          .store_update_state(jid, list(
            state = "RETRY",
            retries = retries + 1L
          ))
          .audit_log(jid, "retry", list(
            step_index = state$step_index, exit_code = exit_code
          ))
          # Re-dispatch
          tryCatch(.executor_start(jid), error = function(e) {
            .store_update_state(jid, list(
              state = "FAILED",
              error = paste("Retry dispatch failed:", conditionMessage(e)),
              finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
            ))
          })
        } else {
          .store_update_state(jid, list(
            state = "FAILED",
            error = paste("Step", state$step_index, "failed with exit code", exit_code),
            finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
          ))
          .audit_log(jid, "failed", list(
            step_index = state$step_index, exit_code = exit_code
          ))
        }
      }

      # Clean up worker reference
      rm(list = worker_key, envir = .dsjobs_env)
    }
  }

  invisible(NULL)
}
