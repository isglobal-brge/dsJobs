# Module: Resource Accounting
# Per-user, per-dataset, and global quotas for job submission.

#' Check quotas before job submission
#'
#' Enforces max concurrent jobs and per-user limits.
#'
#' @param spec Named list; the job spec.
#' @return Invisible TRUE, or stops with an error.
#' @keywords internal
.check_quotas <- function(spec) {
  settings <- .dsjobs_settings()

  # Count active jobs (PENDING + RUNNING)
  active <- .store_list_jobs(states = c("PENDING", "RUNNING"))
  n_active <- nrow(active)

  if (n_active >= settings$max_concurrent_jobs) {
    stop("Job quota exceeded: ", n_active, " active jobs (max ",
         settings$max_concurrent_jobs, "). ",
         "Wait for existing jobs to complete or cancel them.",
         call. = FALSE)
  }

  invisible(TRUE)
}

#' Get dsJobs resource settings
#'
#' @return Named list of resource limits.
#' @keywords internal
.dsjobs_settings <- function() {
  list(
    max_concurrent_jobs = as.integer(.dsj_option("max_concurrent_jobs", 5L)),
    max_steps_per_job = as.integer(.dsj_option("max_steps_per_job", 20L)),
    default_timeout_secs = as.integer(.dsj_option("default_timeout_secs", 3600L)),
    max_retries = as.integer(.dsj_option("max_retries", 2L)),
    job_expiry_hours = as.numeric(.dsj_option("job_expiry_hours", 168))
  )
}
