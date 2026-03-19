# Module: Step Dispatch
# Starts, advances, and kills job step execution.

#' Start executing a job
#'
#' Transitions from PENDING to RUNNING and executes the first step.
#'
#' @param job_id Character; the job ID.
#' @return Invisible TRUE.
#' @keywords internal
.executor_start <- function(job_id) {
  state <- .store_read_state(job_id)
  spec <- .store_read_spec(job_id)

  if (is.null(state) || is.null(spec)) {
    stop("Job not found: ", job_id, call. = FALSE)
  }

  if (!state$state %in% c("PENDING", "RETRY")) {
    return(invisible(FALSE))
  }

  step_index <- if (identical(state$state, "RETRY")) {
    as.integer(state$step_index)
  } else {
    1L
  }

  .store_update_state(job_id, list(
    state = "RUNNING",
    step_index = step_index,
    started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  ))
  .audit_log(job_id, "started", list(step_index = step_index))

  step <- spec$steps[[step_index]]
  .execute_step(job_id, step_index, step)

  invisible(TRUE)
}

#' Advance to the next step or finish the job
#'
#' Called after a step completes successfully.
#'
#' @param job_id Character; the job ID.
#' @param state Named list; current state.
#' @param spec Named list; job spec.
#' @return Invisible NULL.
#' @keywords internal
.executor_advance <- function(job_id, state, spec) {
  current_step <- as.integer(state$step_index)
  total_steps <- length(spec$steps)

  if (current_step >= total_steps) {
    # All steps complete
    .store_update_state(job_id, list(
      state = "FINISHED",
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
    ))
    .audit_log(job_id, "finished", list(total_steps = total_steps))

    # Auto-publish if spec has publish config
    if (!is.null(spec$publish)) {
      tryCatch({
        .publish_safe_result(job_id, spec)
        .store_update_state(job_id, list(state = "PUBLISHED"))
        .audit_log(job_id, "published", list())
      }, error = function(e) {
        .audit_log(job_id, "publish_failed", list(error = conditionMessage(e)))
      })
    }

    return(invisible(NULL))
  }

  # Advance to next step
  next_step <- current_step + 1L
  .store_update_state(job_id, list(step_index = next_step))

  step <- spec$steps[[next_step]]
  .execute_step(job_id, next_step, step)

  invisible(NULL)
}

#' Execute a single step
#'
#' Dispatches to session or artifact runner based on step plane.
#'
#' @param job_id Character; the job ID.
#' @param step_index Integer; the step index.
#' @param step Named list; the step spec.
#' @return Invisible NULL.
#' @keywords internal
.execute_step <- function(job_id, step_index, step) {
  step_dir <- .store_step_dir(job_id, step_index)

  # Write step status
  status <- list(
    step_index = step_index,
    type = step$type,
    plane = step$plane,
    state = "running",
    started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
  jsonlite::write_json(status, file.path(step_dir, "status.json"),
                        auto_unbox = TRUE, pretty = TRUE)

  if (identical(step$plane, "session")) {
    .run_session_step(job_id, step_index, step, step_dir)
  } else {
    .run_artifact_step(job_id, step_index, step, step_dir)
  }

  invisible(NULL)
}

#' Kill a job's active worker
#'
#' @param job_id Character; the job ID.
#' @return Invisible TRUE.
#' @keywords internal
.executor_kill <- function(job_id) {
  worker_key <- paste0("worker_", job_id)
  proc <- tryCatch(
    get(worker_key, envir = .dsjobs_env),
    error = function(e) NULL
  )

  if (!is.null(proc) && inherits(proc, "process") && proc$is_alive()) {
    proc$signal(15L)
    proc$wait(timeout = 5000)
    if (proc$is_alive()) proc$kill()
  }

  if (exists(worker_key, envir = .dsjobs_env)) {
    rm(list = worker_key, envir = .dsjobs_env)
  }

  invisible(TRUE)
}
