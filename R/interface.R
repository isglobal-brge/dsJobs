# Module: DataSHIELD Methods
# Aggregate = returns data to client. Assign = puts data in server session.
# No scheduling triggered. Worker daemon handles dispatch independently.

# --- Handle resolution ---

#' @keywords internal
.resolve_job_id <- function(x) {
  if (is.character(x) && length(x) == 1 && startsWith(x, "job_")) return(x)
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

# =============================================================================
# ASSIGN methods
# =============================================================================

#' Submit a Job
#'
#' DataSHIELD ASSIGN method. Creates the job in SQLite. The external
#' worker daemon will pick it up.
#'
#' @param spec_encoded Character; B64/JSON-encoded job specification.
#' @return Job handle (list with job_id, state).
#' @export
jobSubmitDS <- function(spec_encoded) {
  spec <- .ds_arg(spec_encoded)
  spec <- .validate_job_spec(spec)
  owner_id <- .get_owner_id(spec$.owner)
  job_id <- .generate_job_id()

  db <- .db_connect()
  on.exit(.db_close(db))

  .check_quotas(db, owner_id)
  .store_create_job(db, job_id, owner_id, spec, length(spec$steps))

  # Ensure worker is running (convenience -- in production, systemd handles this)
  tryCatch(.dsjobs_worker_start(), error = function(e) NULL)

  list(job_id = job_id, state = "PENDING",
       submitted_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
}

#' Cancel a Job
#'
#' DataSHIELD ASSIGN method.
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @return Updated handle.
#' @export
jobCancelDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  .assert_owner(db, job_id, mode = "write")

  job <- .store_get_job(db, job_id)
  if (job$state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED"))
    stop("Job already in terminal state: ", job$state, call. = FALSE)

  .executor_kill(db, job_id)
  .store_update_job(db, job_id, state = "CANCELLED", worker_pid = NA_integer_,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
  .db_log_event(db, job_id, "cancelled")
  list(job_id = job_id, state = "CANCELLED")
}

#' Load a Job Output into the Server Session
#'
#' DataSHIELD ASSIGN method. Reads a specific output from a completed
#' job and returns it as an R object that gets assigned to a symbol
#' in the DataSHIELD session. The researcher never sees the data
#' directly -- it stays on the server. But it can be used as input
#' for dsFlower, dsImaging, another job, etc.
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @param output_name Character; name of the output to load.
#' @return The R object stored in that output.
#' @export
jobLoadOutputDS <- function(job_id_or_symbol, output_name) {
  job_id <- .resolve_job_id(job_id_or_symbol)

  db <- .db_connect()
  on.exit(.db_close(db))
  .assert_owner(db, job_id)

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    stop("Job not finished (state: ", job$state, ").", call. = FALSE)

  # Find the output
  out <- DBI::dbGetQuery(db,
    "SELECT path_or_ref, kind FROM outputs
     WHERE job_id = ? AND name = ?
     ORDER BY id DESC LIMIT 1",
    params = list(job_id, output_name))

  if (nrow(out) == 0)
    stop("Output '", output_name, "' not found for job ", job_id, ".", call. = FALSE)

  path <- out$path_or_ref[1]
  if (is.na(path) || !file.exists(path))
    stop("Output file not found on disk.", call. = FALSE)

  # Load and return -- this becomes the assigned value in the DS session
  if (grepl("\\.rds$", path, ignore.case = TRUE)) {
    return(readRDS(path))
  }
  # For non-RDS files, return a reference descriptor
  list(
    type = "job_output_ref",
    job_id = job_id,
    output_name = output_name,
    kind = out$kind[1],
    path = path
  )
}

# =============================================================================
# AGGREGATE methods
# =============================================================================

#' Get Job Status
#'
#' DataSHIELD AGGREGATE method. Returns status info to the client.
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
  if (is.null(job)) stop("Job not found.", call. = FALSE)

  list(
    job_id = job$job_id, state = job$state,
    step_index = as.integer(job$step_index),
    total_steps = as.integer(job$total_steps),
    label = job$label, tags = job$tags,
    submitted_at = job$submitted_at, started_at = job$started_at,
    finished_at = job$finished_at, error = job$error_message,
    retries = as.integer(job$retry_count))
}

#' Get Job Result (client-safe outputs only)
#'
#' DataSHIELD AGGREGATE method. Returns disclosure-safe result to client.
#' Only outputs marked safe_for_client are included with their values.
#' Non-safe outputs are listed by name/kind (loadable via jobLoadOutputDS).
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
  if (is.null(job)) stop("Job not found.", call. = FALSE)

  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    return(list(job_id = job_id, state = job$state, ready = FALSE,
                error = job$error_message))

  # Try cached result
  home <- .dsjobs_home()
  result_path <- file.path(home, "artifacts", job_id, "result", "result.rds")
  if (file.exists(result_path)) {
    result <- readRDS(result_path)
    result$ready <- TRUE
    return(result)
  }

  # Build on the fly
  .build_job_result(db, job_id)
}

#' Get Job Logs
#'
#' DataSHIELD AGGREGATE method. Returns sanitized logs.
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @param last_n Integer; max lines.
#' @return Character vector.
#' @export
jobLogsDS <- function(job_id_or_symbol, last_n = 50L) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  last_n <- as.integer(last_n %||% 50L)
  db <- .db_connect()
  on.exit(.db_close(db))
  .assert_owner(db, job_id)

  home <- .dsjobs_home()
  lines <- character(0)
  art_dir <- file.path(home, "artifacts", job_id)
  if (dir.exists(art_dir)) {
    step_dirs <- sort(list.dirs(art_dir, full.names = TRUE, recursive = FALSE))
    step_dirs <- step_dirs[grepl("^step_", basename(step_dirs))]
    for (sd in step_dirs) {
      for (lf in c("stdout.log", "stderr.log")) {
        lp <- file.path(sd, lf)
        if (file.exists(lp)) {
          sl <- readLines(lp, warn = FALSE)
          if (length(sl) > 0)
            lines <- c(lines, paste0("[", basename(sd), "/", lf, "] ", sl))
        }
      }
    }
  }
  .sanitize_job_logs(lines, last_n)
}

#' List Jobs (owned by current user)
#'
#' DataSHIELD AGGREGATE method. Optionally filtered by label so that
#' domain packages (dsRadiomicsClient, dsImagingClient) can show only
#' their own jobs.
#'
#' @param label Character or NULL; filter by label (e.g. "dsRadiomics").
#' @return Data.frame.
#' @export
jobListDS <- function(label = NULL) {
  owner_id <- .get_owner_id()
  db <- .db_connect()
  on.exit(.db_close(db))
  jobs <- .store_list_jobs(db, owner_id = owner_id, label = label)
  if (nrow(jobs) == 0)
    return(data.frame(job_id = character(0), state = character(0),
      label = character(0), visibility = character(0),
      owner_id = character(0), submitted_at = character(0),
      progress = character(0), stringsAsFactors = FALSE))
  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  jobs[, c("job_id", "state", "label", "visibility", "owner_id",
           "submitted_at", "progress"), drop = FALSE]
}

#' List Available Outputs for a Job
#'
#' DataSHIELD AGGREGATE method. Returns names and kinds of all outputs
#' from a completed job. Client uses this to decide what to load via
#' jobLoadOutputDS (assign) or inspect via jobResultDS (aggregate).
#'
#' @param job_id_or_symbol Character; job ID or handle symbol.
#' @return Data.frame with name, kind, safe_for_client columns.
#' @export
jobOutputsDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  .assert_owner(db, job_id)

  DBI::dbGetQuery(db,
    "SELECT name, kind, safe_for_client, size_bytes FROM outputs
     WHERE job_id = ? ORDER BY id",
    params = list(job_id))
}

#' Get Server Job Capabilities
#'
#' DataSHIELD AGGREGATE method.
#'
#' @return Named list.
#' @export
jobCapabilitiesDS <- function() {
  settings <- .dsjobs_settings()
  trust <- .dsjobs_trust_profile()
  runners <- .list_runners()

  runner_details <- lapply(runners, function(r) {
    cfg <- .load_runner_config(r)
    if (is.null(cfg)) return(list(name = r))
    list(name = cfg$name %||% r, plane = cfg$plane %||% "artifact",
         resource_class = cfg$resource_class %||% "default")
  })
  names(runner_details) <- runners

  home <- .dsjobs_home(must_exist = FALSE)
  worker_running <- FALSE
  if (!is.null(home)) {
    pf <- file.path(home, "worker.pid")
    if (file.exists(pf)) {
      pid <- tryCatch(as.integer(readLines(pf, n = 1, warn = FALSE)),
                       error = function(e) NA_integer_)
      worker_running <- .pid_is_alive(pid)
    }
  }

  list(dsjobs_version = as.character(utils::packageVersion("dsJobs")),
       runners = runner_details,
       publishers = .list_publishers(),
       max_jobs_per_user = settings$max_jobs_per_user,
       max_jobs_global = settings$max_jobs_global,
       max_steps_per_job = settings$max_steps_per_job,
       privacy_profile = trust$name,
       worker_running = worker_running)
}
