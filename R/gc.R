# Module: Garbage Collection
# Removes expired jobs and orphaned staging artifacts.

#' Remove expired jobs from the queue
#'
#' Jobs in terminal states (FINISHED, PUBLISHED, FAILED, CANCELLED)
#' older than dsjobs.job_expiry_hours are removed.
#'
#' @return Integer; number of jobs removed.
#' @keywords internal
.gc_expired_jobs <- function() {
  settings <- .dsjobs_settings()
  expiry_hours <- settings$job_expiry_hours
  home <- .dsjobs_home(must_exist = FALSE)
  if (is.null(home) || !dir.exists(home)) return(0L)

  terminal_states <- c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED")
  all_jobs <- .store_list_jobs(states = terminal_states)
  if (nrow(all_jobs) == 0) return(0L)

  removed <- 0L
  for (i in seq_len(nrow(all_jobs))) {
    jid <- all_jobs$job_id[i]
    state <- .store_read_state(jid)
    if (is.null(state)) next

    finished_at <- state$finished_at
    if (is.null(finished_at)) next

    finished_time <- tryCatch(
      as.POSIXct(finished_at, format = "%Y-%m-%dT%H:%M:%S"),
      error = function(e) NA
    )
    if (is.na(finished_time)) next

    age_hours <- as.numeric(difftime(Sys.time(), finished_time, units = "hours"))
    if (age_hours > expiry_hours) {
      job_dir <- file.path(home, "queue", jid)
      unlink(job_dir, recursive = TRUE)
      .audit_log(jid, "gc_removed", list(age_hours = round(age_hours, 1)))
      removed <- removed + 1L
    }
  }

  removed
}

#' Remove orphaned staging directories
#'
#' Cleans up DSJOBS_HOME/publish/staging/ entries that don't correspond
#' to any active job.
#'
#' @return Integer; number of staging dirs removed.
#' @keywords internal
.gc_orphaned_staging <- function() {
  home <- .dsjobs_home(must_exist = FALSE)
  if (is.null(home) || !dir.exists(home)) return(0L)

  staging_dir <- file.path(home, "publish", "staging")
  if (!dir.exists(staging_dir)) return(0L)

  dirs <- list.dirs(staging_dir, full.names = TRUE, recursive = FALSE)
  if (length(dirs) == 0) return(0L)

  # Get active job IDs
  active <- .store_list_jobs(states = c("PENDING", "RUNNING"))
  active_ids <- if (nrow(active) > 0) active$job_id else character(0)

  removed <- 0L
  for (d in dirs) {
    # Extract job_id from directory name (format: jobid_assetname)
    dir_name <- basename(d)
    parts <- strsplit(dir_name, "_(?=[^_]+$)", perl = TRUE)[[1]]
    job_id_part <- parts[1]

    # Check if orphaned and older than 1 hour
    info <- file.info(d)
    if (is.na(info$mtime)) next
    age_hours <- as.numeric(difftime(Sys.time(), info$mtime, units = "hours"))

    if (age_hours > 1 && !job_id_part %in% active_ids) {
      unlink(d, recursive = TRUE)
      removed <- removed + 1L
    }
  }

  removed
}
