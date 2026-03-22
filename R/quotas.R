# Module: Resource Accounting

#' @keywords internal
.check_quotas <- function(db, owner_id) {
  settings <- .dsjobs_settings()
  user_n <- DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs WHERE owner_id = ? AND state IN ('PENDING','RUNNING')",
    params = list(owner_id))$n
  if (user_n >= settings$max_jobs_per_user)
    stop("Per-user quota exceeded: ", user_n, " active jobs (max ",
         settings$max_jobs_per_user, ").", call. = FALSE)
  global_n <- DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs WHERE state IN ('PENDING','RUNNING')")$n
  if (global_n >= settings$max_jobs_global)
    stop("Global job quota exceeded.", call. = FALSE)
}

#' @keywords internal
.dsjobs_settings <- function() {
  list(
    max_jobs_per_user = as.integer(.dsj_option("max_jobs_per_user", 50L)),
    max_jobs_global = as.integer(.dsj_option("max_jobs_global", 200L)),
    max_steps_per_job = as.integer(.dsj_option("max_steps_per_job", 20L)),
    max_spec_bytes = as.integer(.dsj_option("max_spec_bytes", 1048576L)),
    default_timeout_secs = as.integer(.dsj_option("default_timeout_secs", 3600L)),
    max_retries = as.integer(.dsj_option("max_retries", 2L)),
    job_expiry_hours = as.numeric(.dsj_option("job_expiry_hours", 168)),
    worker_poll_secs = as.numeric(.dsj_option("worker_poll_secs", 2)))
}
