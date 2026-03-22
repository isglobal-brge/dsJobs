# Module: Public API for Plugin Packages
#
# Functions that domain packages (dsRadiomics, etc.) need to interact
# with dsJobs without accessing internal functions via :::.

#' Query failed jobs by tag
#'
#' Returns jobs in FAILED state that match a tag pattern.
#' Used by domain packages to sync failure states back to their
#' own tracking systems (e.g. asset_items in dsImaging).
#'
#' @param tag_pattern Character; pattern to match against job tags (SQL LIKE).
#' @return data.frame with columns: job_id, tags, error_message.
#' @export
query_failed_jobs <- function(tag_pattern) {
  db <- .db_connect()
  on.exit(.db_close(db))
  DBI::dbGetQuery(db,
    "SELECT job_id, tags, error_message FROM jobs
     WHERE state = 'FAILED' AND tags LIKE ?",
    params = list(tag_pattern))
}

#' Count active jobs by tag
#'
#' Returns the number of PENDING or RUNNING jobs matching a tag pattern.
#' Used by domain packages for backpressure / drip feed decisions.
#'
#' @param tag_pattern Character; pattern to match (SQL LIKE).
#' @return Integer; count of active jobs.
#' @export
count_active_jobs <- function(tag_pattern) {
  db <- .db_connect()
  on.exit(.db_close(db))
  DBI::dbGetQuery(db,
    "SELECT COUNT(*) AS n FROM jobs
     WHERE state IN ('PENDING','RUNNING') AND tags LIKE ?",
    params = list(tag_pattern))$n
}

#' Get the current owner ID
#'
#' Resolves the owner identity from the session context.
#' Used by domain packages when submitting jobs on behalf of users.
#'
#' @return Character; the owner identifier.
#' @export
get_owner_id <- function() {
  .get_owner_id()
}
