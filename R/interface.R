# Module: DataSHIELD Exposed Methods
# All DataSHIELD assign/aggregate methods for the durable job runtime.

# --- ASSIGN methods ---

#' Submit a Job
#'
#' DataSHIELD ASSIGN method. Validates the job spec, checks quotas,
#' creates the job in the durable store, and triggers the scheduler.
#'
#' @param spec_encoded Character; B64/JSON-encoded job specification.
#' @return A job handle (named list with job_id and status).
#' @export
jobSubmitDS <- function(spec_encoded) {
  spec <- .ds_arg(spec_encoded)

  # Validate
  spec <- .validate_job_spec(spec)

  # Check quotas
  .check_quotas(spec)

  # Create job in store
  job_id <- .generate_job_id()
  .store_create_job(job_id, spec)

  # Trigger scheduler (lazy dispatch)
  .scheduler_dispatch()

  # Return handle
  list(
    job_id = job_id,
    state = "PENDING",
    submitted_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
}

#' Cancel a Job
#'
#' DataSHIELD ASSIGN method. Cancels a running or pending job.
#'
#' @param job_id Character; the job ID to cancel.
#' @return Updated job handle with CANCELLED state.
#' @export
jobCancelDS <- function(job_id) {
  state <- .store_read_state(job_id)
  if (is.null(state)) {
    stop("Job not found: ", job_id, call. = FALSE)
  }

  if (state$state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED")) {
    stop("Job '", job_id, "' is already in terminal state: ", state$state,
         call. = FALSE)
  }

  # Kill any active worker
  .executor_kill(job_id)

  .store_update_state(job_id, list(
    state = "CANCELLED",
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  ))
  .audit_log(job_id, "cancelled", list())

  list(
    job_id = job_id,
    state = "CANCELLED"
  )
}

# --- AGGREGATE methods ---

#' Get Job Status
#'
#' DataSHIELD AGGREGATE method. Returns the current status of a job.
#' Also triggers the scheduler to reap finished workers and dispatch
#' pending jobs (lazy scheduling).
#'
#' @param job_id Character; the job ID.
#' @return Named list with job status information.
#' @export
jobStatusDS <- function(job_id) {
  # Trigger lazy scheduling
  .scheduler_dispatch()

  state <- .store_read_state(job_id)
  if (is.null(state)) {
    stop("Job not found: ", job_id, call. = FALSE)
  }

  list(
    job_id = job_id,
    state = state$state,
    step_index = as.integer(state$step_index %||% 0L),
    total_steps = as.integer(state$total_steps %||% 0L),
    submitted_at = state$submitted_at,
    started_at = state$started_at,
    finished_at = state$finished_at,
    error = state$error,
    retries = as.integer(state$retries %||% 0L)
  )
}

#' Get Job Result
#'
#' DataSHIELD AGGREGATE method. Returns the disclosure-safe result
#' of a completed job.
#'
#' @param job_id Character; the job ID.
#' @return Named list with safe result, or error if not ready.
#' @export
jobResultDS <- function(job_id) {
  state <- .store_read_state(job_id)
  if (is.null(state)) {
    stop("Job not found: ", job_id, call. = FALSE)
  }

  if (!state$state %in% c("FINISHED", "PUBLISHED")) {
    return(list(
      job_id = job_id,
      state = state$state,
      ready = FALSE,
      error = state$error
    ))
  }

  home <- .dsjobs_home()
  result_path <- file.path(home, "queue", job_id, "result", "result.rds")

  if (file.exists(result_path)) {
    result <- readRDS(result_path)
    result$ready <- TRUE
    return(result)
  }

  # Try to publish now if not yet done
  spec <- .store_read_spec(job_id)
  if (!is.null(spec)) {
    tryCatch({
      result <- .publish_safe_result(job_id, spec)
      result$ready <- TRUE
      return(result)
    }, error = function(e) NULL)
  }

  list(
    job_id = job_id,
    state = state$state,
    ready = TRUE,
    summary = list(status = "completed")
  )
}

#' Get Job Logs
#'
#' DataSHIELD AGGREGATE method. Returns sanitized log output for a job.
#'
#' @param job_id Character; the job ID.
#' @param last_n Integer; number of lines to return (max 200).
#' @return Character vector of sanitized log lines.
#' @export
jobLogsDS <- function(job_id, last_n = 50L) {
  last_n <- as.integer(last_n %||% 50L)

  state <- .store_read_state(job_id)
  if (is.null(state)) {
    stop("Job not found: ", job_id, call. = FALSE)
  }

  home <- .dsjobs_home()
  lines <- character(0)

  # Collect logs from step directories
  steps_dir <- file.path(home, "queue", job_id, "steps")
  if (dir.exists(steps_dir)) {
    step_dirs <- sort(list.dirs(steps_dir, full.names = TRUE, recursive = FALSE))

    for (sd in step_dirs) {
      for (logfile in c("stdout.log", "stderr.log")) {
        log_path <- file.path(sd, logfile)
        if (file.exists(log_path)) {
          step_lines <- readLines(log_path, warn = FALSE)
          if (length(step_lines) > 0) {
            prefix <- paste0("[", basename(sd), "/", logfile, "] ")
            lines <- c(lines, paste0(prefix, step_lines))
          }
        }
      }
    }
  }

  .sanitize_job_logs(lines, last_n)
}

#' List All Jobs
#'
#' DataSHIELD AGGREGATE method. Returns a summary of all jobs.
#'
#' @return Data.frame with job_id, state, submitted_at, progress.
#' @export
jobListDS <- function() {
  # Trigger lazy scheduling
  .scheduler_dispatch()

  jobs <- .store_list_jobs()
  if (nrow(jobs) == 0) {
    return(data.frame(
      job_id = character(0), state = character(0),
      submitted_at = character(0), progress = character(0),
      stringsAsFactors = FALSE
    ))
  }

  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  jobs[, c("job_id", "state", "submitted_at", "progress"), drop = FALSE]
}

#' Get Server Job Capabilities
#'
#' DataSHIELD AGGREGATE method. Returns available runners, quotas,
#' and configuration limits.
#'
#' @return Named list of capabilities.
#' @export
jobCapabilitiesDS <- function() {
  settings <- .dsjobs_settings()
  trust <- .dsjobs_trust_profile()
  runners <- .list_runners()

  # Load runner details
  runner_details <- lapply(runners, function(r) {
    cfg <- .load_runner_config(r)
    if (is.null(cfg)) return(list(name = r))
    list(
      name = cfg$name %||% r,
      plane = cfg$plane %||% "artifact",
      resource_class = cfg$resource_class %||% "default",
      timeout_secs = cfg$timeout_secs %||% settings$default_timeout_secs
    )
  })
  names(runner_details) <- runners

  list(
    dsjobs_version = as.character(utils::packageVersion("dsJobs")),
    runners = runner_details,
    max_concurrent_jobs = settings$max_concurrent_jobs,
    max_steps_per_job = settings$max_steps_per_job,
    default_timeout_secs = settings$default_timeout_secs,
    max_retries = settings$max_retries,
    privacy_profile = trust$name,
    active_jobs = nrow(.store_list_jobs(states = c("PENDING", "RUNNING")))
  )
}
