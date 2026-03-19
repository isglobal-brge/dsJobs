# Module: DataSHIELD Methods
# DS methods are READ-ONLY helpers for the shared SQLite.
# Access control is enforced by the Opal/Armadillo filesystem layer,
# NOT by these methods (Rock doesn't know the Opal user).
# Cancel uses the filesystem control plane (client writes cancel request).

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
#' @export
jobSubmitDS <- function(spec_encoded) {
  spec <- .ds_arg(spec_encoded)
  spec <- .validate_job_spec(spec)
  owner_id <- .get_owner_id(spec$.owner)
  job_id <- if (!is.null(spec$job_id) && grepl("^job_", spec$job_id))
    spec$job_id else .generate_job_id()

  db <- .db_connect()
  on.exit(.db_close(db))

  # Skip if job already exists (idempotent -- dual-path submit)
  existing <- .store_get_job(db, job_id)
  if (!is.null(existing)) {
    return(list(job_id = job_id, state = existing$state,
                submitted_at = existing$submitted_at))
  }

  .check_quotas(db, owner_id)
  .store_create_job(db, job_id, owner_id, spec, length(spec$steps))

  # If all steps are session-plane, execute inline (synchronous).
  # Artifact-plane steps are deferred to the worker daemon.
  all_session <- all(vapply(spec$steps, function(s)
    identical(s$plane, "session"), logical(1)))

  if (all_session) {
    # Execute synchronously -- session steps are brief and idempotent
    .store_update_job(db, job_id, state = "RUNNING", step_index = 1L,
      started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "started")
    tryCatch(
      .executor_run_step(db, job_id, 1L, spec),
      error = function(e) {
        .store_update_job(db, job_id, state = "FAILED",
          error_message = conditionMessage(e),
          finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
      }
    )
  } else {
    # Has artifact steps -- needs the worker daemon
    tryCatch(.dsjobs_worker_start(), error = function(e) NULL)
  }

  job <- .store_get_job(db, job_id)
  list(job_id = job_id, state = job$state %||% "PENDING",
       submitted_at = job$submitted_at %||% format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
}

#' Cancel a Job
#' @export
jobCancelDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (job$state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED"))
    stop("Job already in terminal state: ", job$state, call. = FALSE)

  .executor_kill(db, job_id)
  .store_update_job(db, job_id, state = "CANCELLED", worker_pid = NA_integer_,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
  .db_log_event(db, job_id, "cancelled")
  list(job_id = job_id, state = "CANCELLED")
}

#' Load a Job Output into the Server Session
#' @export
jobLoadOutputDS <- function(job_id_or_symbol, output_name) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    stop("Job not finished (state: ", job$state, ").", call. = FALSE)

  out <- DBI::dbGetQuery(db,
    "SELECT path_or_ref, kind FROM outputs WHERE job_id = ? AND name = ?
     ORDER BY id DESC LIMIT 1",
    params = list(job_id, output_name))
  if (nrow(out) == 0)
    stop("Output '", output_name, "' not found for job ", job_id, ".", call. = FALSE)

  path <- out$path_or_ref[1]
  if (is.na(path) || !file.exists(path))
    stop("Output file not found on disk.", call. = FALSE)

  if (grepl("\\.rds$", path, ignore.case = TRUE)) return(readRDS(path))
  list(type = "job_output_ref", job_id = job_id, output_name = output_name,
       kind = out$kind[1], path = path)
}

# =============================================================================
# AGGREGATE methods (read-only, no ownership check)
# =============================================================================

#' Get Job Status
#' @export
jobStatusDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found: ", job_id, call. = FALSE)

  list(job_id = job$job_id, state = job$state,
    step_index = as.integer(job$step_index),
    total_steps = as.integer(job$total_steps),
    label = job$label, tags = job$tags,
    visibility = job$visibility, owner_id = job$owner_id,
    submitted_at = job$submitted_at, started_at = job$started_at,
    finished_at = job$finished_at, error = job$error_message,
    retries = as.integer(job$retry_count))
}

#' Get Job Result
#' @export
jobResultDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)

  if (!job$state %in% c("FINISHED", "PUBLISHED"))
    return(list(job_id = job_id, state = job$state, ready = FALSE,
                error = job$error_message))

  home <- .dsjobs_home()
  result_path <- file.path(home, "artifacts", job_id, "result", "result.rds")
  if (file.exists(result_path)) {
    result <- readRDS(result_path)
    result$ready <- TRUE
    return(result)
  }
  .build_job_result(db, job_id)
}

#' Get Job Logs
#' @export
jobLogsDS <- function(job_id_or_symbol, last_n = 50L) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  last_n <- as.integer(last_n %||% 50L)
  db <- .db_connect()
  on.exit(.db_close(db))

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

#' List Jobs
#'
#' Returns global jobs + jobs owned by caller_id (if provided).
#' Private jobs of OTHER users are hidden. The caller_id is provided
#' by the client based on the authenticated Opal/Armadillo username.
#' It cannot be spoofed in practice because the filesystem control
#' plane prevents submitting jobs as another user.
#'
#' @param label Character or NULL; filter by label.
#' @param caller_id Character or NULL; the authenticated username from client.
#' @export
jobListDS <- function(label = NULL, caller_id = NULL) {
  db <- .db_connect()
  on.exit(.db_close(db))

  if (is.null(caller_id) || !nzchar(caller_id)) {
    # No caller_id: show only global jobs (safe default)
    jobs <- .store_list_jobs(db, label = label)
    if (nrow(jobs) > 0) {
      jobs <- jobs[jobs$visibility == "global" | is.na(jobs$visibility), , drop = FALSE]
    }
  } else {
    # Show caller's own jobs + all global jobs
    jobs <- .store_list_jobs(db, label = label)
    if (nrow(jobs) > 0) {
      jobs <- jobs[jobs$owner_id == caller_id |
                    jobs$visibility == "global" |
                    is.na(jobs$visibility), , drop = FALSE]
    }
  }
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
#' @export
jobOutputsDS <- function(job_id_or_symbol) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  DBI::dbGetQuery(db,
    "SELECT name, kind, safe_for_client, size_bytes FROM outputs
     WHERE job_id = ? ORDER BY id",
    params = list(job_id))
}

#' Get Server Job Capabilities
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
       runners = runner_details, publishers = .list_publishers(),
       max_jobs_per_user = settings$max_jobs_per_user,
       max_jobs_global = settings$max_jobs_global,
       max_steps_per_job = settings$max_steps_per_job,
       privacy_profile = trust$name, worker_running = worker_running)
}
