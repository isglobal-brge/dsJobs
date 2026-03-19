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

#' Verify access token for a job
#'
#' For private jobs: token required. For global jobs: token not required
#' for read operations (safe results). Write operations (cancel) always
#' require token.
#'
#' @param db DBI connection.
#' @param job Named list from .store_get_job().
#' @param access_token Character; the plaintext token from client.
#' @param require_for_global Logical; if TRUE, require token even for global jobs.
#' @keywords internal
.verify_token <- function(db, job, access_token, require_for_global = FALSE) {
  is_global <- identical(job$visibility, "global")

  # Global jobs: read access without token (safe results only)
  if (is_global && !require_for_global) return(invisible(TRUE))

  # Private jobs or write operations: token required
  if (is.null(access_token) || !nzchar(access_token))
    stop("Access denied: access_token required for this job.", call. = FALSE)

  stored_hash <- job$access_token_hash
  if (is.null(stored_hash) || is.na(stored_hash))
    return(invisible(TRUE))  # Legacy jobs without token

  provided_hash <- .hash_token(access_token)
  if (!identical(provided_hash, stored_hash))
    stop("Access denied: invalid access_token.", call. = FALSE)

  invisible(TRUE)
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

  # Client generates token, sends hash. Server stores hash only.
  token_hash <- spec$.access_token_hash

  # Global job deduplication by spec_hash
  if (identical(spec$visibility, "global")) {
    spec_for_hash <- spec[setdiff(names(spec), c("job_id", ".owner", ".access_token_hash"))]
    spec_hash <- digest::digest(jsonlite::toJSON(spec_for_hash, auto_unbox = TRUE),
                                 algo = "sha256", serialize = FALSE)
    existing_dup <- DBI::dbGetQuery(db,
      "SELECT job_id, state FROM jobs
       WHERE spec_hash = ? AND visibility = 'global'
         AND state IN ('FINISHED', 'PUBLISHED')
       LIMIT 1",
      params = list(spec_hash))
    if (nrow(existing_dup) > 0) {
      # Dedup: create a lightweight entry for the new job_id that
      # mirrors the existing job's state. This way the client's
      # job_id and access_token work normally.
      existing_job <- .store_get_job(db, existing_dup$job_id[1])
      .store_create_job(db, job_id, owner_id, spec, length(spec$steps),
        access_token_hash = token_hash, spec_hash = spec_hash)
      .store_update_job(db, job_id,
        state = existing_job$state,
        step_index = as.integer(existing_job$step_index),
        started_at = existing_job$started_at,
        finished_at = existing_job$finished_at)
      .db_log_event(db, job_id, "deduplicated",
        list(original_job_id = existing_dup$job_id[1]))

      # Copy outputs from existing job
      existing_outputs <- DBI::dbGetQuery(db,
        "SELECT name, kind, path_or_ref, size_bytes, safe_for_client
         FROM outputs WHERE job_id = ?",
        params = list(existing_dup$job_id[1]))
      for (i in seq_len(nrow(existing_outputs))) {
        o <- existing_outputs[i, ]
        .db_register_output(db, job_id, NA_integer_, o$name, o$kind,
          o$path_or_ref, o$size_bytes, as.logical(o$safe_for_client))
      }

      job <- .store_get_job(db, job_id)
      return(list(job_id = job_id, state = job$state,
                   deduplicated = TRUE,
                   submitted_at = job$submitted_at))
    }
  } else {
    spec_hash <- NULL
  }

  .store_create_job(db, job_id, owner_id, spec, length(spec$steps),
                     access_token_hash = token_hash, spec_hash = spec_hash)

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
  list(job_id = job_id,
       state = job$state %||% "PENDING",
       submitted_at = job$submitted_at %||% format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
}

#' Cancel a Job
#' @export
jobCancelDS <- function(job_id_or_symbol, access_token = NULL) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  .verify_token(db, job, access_token, require_for_global = TRUE)
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
#' When \code{as_descriptor = TRUE} and the output is a Parquet file,
#' returns a \code{FlowerDatasetDescriptor} instead of loading the data
#' into memory. This enables zero-copy column projection downstream.
#'
#' @param job_id_or_symbol Character; job ID or symbol name
#' @param output_name Character; name of the output to load
#' @param access_token Character; access token for the job
#' @param as_descriptor Logical; if TRUE and output is Parquet, return a
#'   FlowerDatasetDescriptor instead of loading data into memory
#' @export
jobLoadOutputDS <- function(job_id_or_symbol, output_name,
                             access_token = NULL, as_descriptor = FALSE) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  .verify_token(db, job, access_token)
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

  # Descriptor mode: return a FlowerDatasetDescriptor for Parquet outputs
  if (isTRUE(as_descriptor) && grepl("\\.parquet$", path, ignore.case = TRUE)) {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("arrow package required for as_descriptor = TRUE.", call. = FALSE)
    }
    pf <- arrow::read_parquet(path, as_data_frame = FALSE)
    col_names <- names(pf)
    n_rows <- nrow(pf)

    desc <- list(
      dataset_id  = paste0("dsjobs.", job_id, ".", output_name),
      source_kind = "staged_parquet",
      metadata    = list(
        file    = path,
        format  = "parquet",
        n_rows  = n_rows,
        columns = col_names
      ),
      staged_token = paste0("job_", job_id),
      origin       = "dsJobs"
    )
    class(desc) <- "FlowerDatasetDescriptor"
    return(desc)
  }

  # Load the file as an R object based on extension
  obj <- if (grepl("\\.rds$", path, ignore.case = TRUE)) {
    readRDS(path)
  } else if (grepl("\\.csv$", path, ignore.case = TRUE)) {
    utils::read.csv(path, stringsAsFactors = FALSE)
  } else if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    if (requireNamespace("arrow", quietly = TRUE))
      as.data.frame(arrow::read_parquet(path))
    else stop("arrow package required for Parquet files.", call. = FALSE)
  } else if (grepl("\\.json$", path, ignore.case = TRUE)) {
    jsonlite::fromJSON(readLines(path, warn = FALSE), simplifyVector = TRUE)
  } else {
    list(type = "job_output_ref", job_id = job_id, output_name = output_name,
         kind = out$kind[1], path = path)
  }

  obj
}

# =============================================================================
# AGGREGATE methods (read-only, no ownership check)
# =============================================================================

#' Get Job Status
#' @export
jobStatusDS <- function(job_id_or_symbol, access_token = NULL) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found: ", job_id, call. = FALSE)
  .verify_token(db, job, access_token)

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
jobResultDS <- function(job_id_or_symbol, access_token = NULL) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  .verify_token(db, job, access_token)

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
jobLogsDS <- function(job_id_or_symbol, last_n = 50L, access_token = NULL) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  last_n <- as.integer(last_n %||% 50L)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  .verify_token(db, job, access_token)

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
jobOutputsDS <- function(job_id_or_symbol, access_token = NULL) {
  job_id <- .resolve_job_id(job_id_or_symbol)
  db <- .db_connect()
  on.exit(.db_close(db))
  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  .verify_token(db, job, access_token)
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

  worker_health <- .dsjobs_worker_health()

  list(dsjobs_version = as.character(utils::packageVersion("dsJobs")),
       runners = runner_details, publishers = .list_publishers(),
       max_jobs_per_user = settings$max_jobs_per_user,
       max_jobs_global = settings$max_jobs_global,
       max_steps_per_job = settings$max_steps_per_job,
       privacy_profile = trust$name, worker = worker_health,
       admin_enabled = .admin_is_configured())
}

# =============================================================================
# Admin methods (disabled by default, enabled by dsjobs.admin_key option)
# =============================================================================

#' Verify admin key. Disabled if no key configured.
#' Key arrives B64-encoded from client to avoid Opal parser issues.
#' @keywords internal
.verify_admin_key <- function(admin_key) {
  expected <- .dsj_option("admin_key", NULL)

  if (is.null(expected) || !nzchar(expected))
    stop("Admin access is not enabled on this server.", call. = FALSE)

  # Decode B64 transport
  decoded <- .ds_arg(admin_key)
  if (is.list(decoded)) decoded <- decoded$.admin_key

  if (is.null(decoded) || !nzchar(decoded))
    stop("Access denied: admin_key required.", call. = FALSE)

  if (!identical(decoded, expected))
    stop("Access denied: invalid admin_key.", call. = FALSE)

  invisible(TRUE)
}

#' Check if admin is configured
#' @keywords internal
.admin_is_configured <- function() {
  key <- .dsj_option("admin_key", NULL)
  !is.null(key) && nzchar(key)
}

#' List ALL Jobs (admin only)
#'
#' Disabled by default. Enable by setting dsjobs.admin_key on the server:
#'   dsadmin.set_option(con, "dsjobs.admin_key", "your_secret_key")
#'
#' @param admin_key Character; the admin key.
#' @param label Character or NULL; filter by label.
#' @export
jobAdminListDS <- function(admin_key = NULL, label = NULL) {
  .verify_admin_key(admin_key)
  db <- .db_connect()
  on.exit(.db_close(db))
  jobs <- .store_list_jobs(db, label = label)
  if (nrow(jobs) == 0)
    return(data.frame(job_id = character(0), state = character(0),
      label = character(0), visibility = character(0),
      owner_id = character(0), submitted_at = character(0),
      progress = character(0), stringsAsFactors = FALSE))
  jobs$progress <- paste0(jobs$step_index, "/", jobs$total_steps)
  jobs[, c("job_id", "state", "label", "visibility", "owner_id",
           "submitted_at", "progress"), drop = FALSE]
}

#' Cancel Any Job (admin only)
#'
#' Disabled by default. Enable by setting dsjobs.admin_key.
#'
#' @param job_id Character; job ID.
#' @param admin_key Character; the admin key.
#' @export
jobAdminCancelDS <- function(job_id, admin_key = NULL) {
  .verify_admin_key(admin_key)
  job_id <- .resolve_job_id(job_id)
  db <- .db_connect()
  on.exit(.db_close(db))

  job <- .store_get_job(db, job_id)
  if (is.null(job)) stop("Job not found.", call. = FALSE)
  if (job$state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED"))
    stop("Job already in terminal state: ", job$state, call. = FALSE)

  .executor_kill(db, job_id)
  .store_update_job(db, job_id, state = "CANCELLED", worker_pid = NA_integer_,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
  .db_log_event(db, job_id, "admin_cancelled")
  list(job_id = job_id, state = "CANCELLED")
}

