# Module: DataSHIELD Exposed Methods
# All methods open/close their own DB connection.
# No method triggers the scheduler -- the worker daemon handles dispatch.

#' Resolve a job_id from either a raw string or a handle symbol
#' @keywords internal
.resolve_job_id <- function(x) {
  # Direct job_id string
  if (is.character(x) && length(x) == 1 && startsWith(x, "job_")) return(x)

  # Try to look up as handle symbol in calling environments
  if (is.character(x) && length(x) == 1) {
    for (depth in 1:3) {
      env <- tryCatch(sys.frame(-(depth)), error = function(e) NULL)
      if (!is.null(env) && exists(x, envir = env, inherits = FALSE)) {
        obj <- get(x, envir = env, inherits = FALSE)
        if (is.list(obj) && !is.null(obj$job_id)) return(obj$job_id)
      }
    }
    if (exists(x, envir = .GlobalEnv, inherits = FALSE)) {
      obj <- get(x, envir = .GlobalEnv, inherits = FALSE)
      if (is.list(obj) && !is.null(obj$job_id)) return(obj$job_id)
    }
  }
  x
}

# --- ASSIGN methods ---

#' Submit a Job
#'
#' DataSHIELD ASSIGN method.
#'
#' @param spec_encoded Character; B64/JSON-encoded job specification.
#' @return Job handle (list with job_id).
#' @export
jobSubmitDS <- function(spec_encoded) {
  spec <- .ds_arg(spec_encoded)
  spec <- .validate_job_spec(spec)

  owner_id <- .get_owner_id()
  job_id <- .generate_job_id()

  db <- .db_connect()
  on.exit(.db_close(db))

  .check_quotas(db, owner_id)
  .store_create_job(db, job_id, owner_id, spec, length(spec$steps))

  # Ensure worker is running
  tryCatch(.dsjobs_worker_start(), error = function(e) NULL)

  list(
    job_id = job_id,
    state = "PENDING",
    submitted_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
}

#' Cancel a Job
#'
#' DataSHIELD ASSIGN method.
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @return Updated handle with CANCELLED state.
#' @export
jobCancelDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)

  db <- .db_connect()
  on.exit(.db_close(db))

  .assert_owner(db, job_id)

  job <- .store_get_job(db, job_id)
  if (job$state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED")) {
    stop("Job '", job_id, "' is already in terminal state: ", job$state,
         call. = FALSE)
  }

  .executor_kill(db, job_id)

  .store_update_job(db, job_id,
    state = "CANCELLED",
    worker_pid = NA_integer_,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
  .db_log_event(db, job_id, "cancelled")

  list(job_id = job_id, state = "CANCELLED")
}

# --- AGGREGATE methods ---

#' Get Job Status
#'
#' DataSHIELD AGGREGATE method.
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @return Named list with job status.
#' @export
jobStatusDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)

  db <- .db_connect()
  on.exit(.db_close(db))

  .assert_owner(db, job_id)

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found: ", job_id, call. = FALSE)

  list(
    job_id = job$job_id,
    state = job$state,
    step_index = as.integer(job$step_index),
    total_steps = as.integer(job$total_steps),
    submitted_at = job$submitted_at,
    started_at = job$started_at,
    finished_at = job$finished_at,
    error = job$error,
    retries = as.integer(job$retries)
  )
}

#' Get Job Result
#'
#' DataSHIELD AGGREGATE method.
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @return Named list with safe result.
#' @export
jobResultDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)

  db <- .db_connect()
  on.exit(.db_close(db))

  .assert_owner(db, job_id)

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found: ", job_id, call. = FALSE)

  if (!job$state %in% c("FINISHED", "PUBLISHED")) {
    return(list(job_id = job_id, state = job$state, ready = FALSE,
                error = job$error))
  }

  home <- .dsjobs_home()
  result_path <- file.path(home, "artifacts", job_id, "result", "result.rds")

  if (file.exists(result_path)) {
    result <- readRDS(result_path)
    result$ready <- TRUE
    return(result)
  }

  # Try to publish now
  spec <- .store_get_spec(db, job_id)
  if (!is.null(spec)) {
    tryCatch({
      result <- .publish_safe_result(job_id, spec, db)
      result$ready <- TRUE
      return(result)
    }, error = function(e) NULL)
  }

  list(job_id = job_id, state = job$state, ready = TRUE,
       summary = list(status = "completed"))
}

#' Get Job Logs
#'
#' DataSHIELD AGGREGATE method.
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @param last_n Integer; max lines (default 50).
#' @return Character vector of sanitized log lines.
#' @export
jobLogsDS <- function(job_id_or_symbol, last_n = 50L) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  last_n <- as.integer(last_n %||% 50L)

  db <- .db_connect()
  on.exit(.db_close(db))

  .assert_owner(db, job_id)

  home <- .dsjobs_home()
  lines <- character(0)

  # Collect logs from artifact step directories
  artifact_dir <- file.path(home, "artifacts", job_id)
  if (dir.exists(artifact_dir)) {
    step_dirs <- sort(list.dirs(artifact_dir, full.names = TRUE, recursive = FALSE))
    step_dirs <- step_dirs[grepl("^step_", basename(step_dirs))]

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

#' List All Jobs (owned by current user)
#'
#' DataSHIELD AGGREGATE method.
#'
#' @return Data.frame with job_id, state, submitted_at, progress.
#' @export
jobListDS <- function() {
  owner_id <- .get_owner_id()

  db <- .db_connect()
  on.exit(.db_close(db))

  jobs <- .store_list_jobs(db, owner_id = owner_id)
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
#' DataSHIELD AGGREGATE method.
#'
#' @return Named list of capabilities.
#' @export
jobCapabilitiesDS <- function() {
  settings <- .dsjobs_settings()
  trust <- .dsjobs_trust_profile()
  runners <- .list_runners()

  runner_details <- lapply(runners, function(r) {
    cfg <- .load_runner_config(r)
    if (is.null(cfg)) return(list(name = r))
    list(name = cfg$name %||% r, plane = cfg$plane %||% "artifact",
         resource_class = cfg$resource_class %||% "default",
         timeout_secs = cfg$timeout_secs %||% settings$default_timeout_secs)
  })
  names(runner_details) <- runners

  # Check worker status
  home <- .dsjobs_home(must_exist = FALSE)
  worker_running <- FALSE
  if (!is.null(home)) {
    pid_file <- file.path(home, "worker.pid")
    if (file.exists(pid_file)) {
      pid <- tryCatch(as.integer(readLines(pid_file, n = 1, warn = FALSE)),
                       error = function(e) NA_integer_)
      worker_running <- .pid_is_alive(pid)
    }
  }

  list(
    dsjobs_version = as.character(utils::packageVersion("dsJobs")),
    runners = runner_details,
    max_jobs_per_user = settings$max_jobs_per_user,
    max_jobs_global = settings$max_jobs_global,
    max_steps_per_job = settings$max_steps_per_job,
    default_timeout_secs = settings$default_timeout_secs,
    max_retries = settings$max_retries,
    privacy_profile = trust$name,
    worker_running = worker_running
  )
}
