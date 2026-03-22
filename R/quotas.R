# Module: Resource Accounting

#' @keywords internal
.check_quotas <- function(db, owner_id) {
  settings <- .dsjobs_settings()
  # Only global limit -- per-user is not enforceable (owner_id is self-reported)
  global_n <- DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs WHERE state IN ('PENDING','RUNNING')")$n
  if (global_n >= settings$max_jobs_global)
    stop("Global job quota exceeded.", call. = FALSE)
}

#' @keywords internal
.dsjobs_settings <- function() {
  list(
    max_jobs_global = as.integer(.dsj_option("max_jobs_global", 1000000L)),
    max_steps_per_job = as.integer(.dsj_option("max_steps_per_job", 50L)),
    max_spec_bytes = as.integer(.dsj_option("max_spec_bytes", 10485760L)),
    default_timeout_secs = as.integer(.dsj_option("default_timeout_secs", 86400L)),
    max_retries = as.integer(.dsj_option("max_retries", 3L)),
    pending_timeout_hours = as.numeric(.dsj_option("pending_timeout_hours", 168)),
    job_expiry_hours = as.numeric(.dsj_option("job_expiry_hours", 720)),
    worker_poll_secs = as.numeric(.dsj_option("worker_poll_secs", 2)))
}
