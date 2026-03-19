# Module: Resource Accounting
# Per-user and global quota enforcement via DB queries.

#' Check quotas before job submission
#' @keywords internal
.check_quotas <- function(db, owner_id) {
  settings <- .dsjobs_settings()

  # Per-user active jobs
  user_active <- DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs
     WHERE owner_id = ? AND state IN ('PENDING', 'RUNNING')",
    params = list(owner_id))$n

  if (user_active >= settings$max_jobs_per_user) {
    stop("Per-user quota exceeded: ", user_active, " active jobs (max ",
         settings$max_jobs_per_user, ").", call. = FALSE)
  }

  # Global active jobs
  global_active <- DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs
     WHERE state IN ('PENDING', 'RUNNING')")$n

  if (global_active >= settings$max_jobs_global) {
    stop("Global job quota exceeded: ", global_active, " active jobs (max ",
         settings$max_jobs_global, ").", call. = FALSE)
  }

  invisible(TRUE)
}

#' Get dsJobs resource settings
#' @keywords internal
.dsjobs_settings <- function() {
  list(
    max_jobs_per_user = as.integer(.dsj_option("max_jobs_per_user", 3L)),
    max_jobs_global = as.integer(.dsj_option("max_jobs_global", 10L)),
    max_steps_per_job = as.integer(.dsj_option("max_steps_per_job", 20L)),
    max_spec_bytes = as.integer(.dsj_option("max_spec_bytes", 1048576L)),
    default_timeout_secs = as.integer(.dsj_option("default_timeout_secs", 3600L)),
    max_retries = as.integer(.dsj_option("max_retries", 2L)),
    job_expiry_hours = as.numeric(.dsj_option("job_expiry_hours", 168)),
    worker_poll_secs = as.numeric(.dsj_option("worker_poll_secs", 2))
  )
}
