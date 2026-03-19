# Module: Audit Trail
# Append-only JSONL audit log per job and global.

#' Append an entry to the job audit log
#'
#' @param job_id Character; the job ID.
#' @param event Character; event type (e.g. "created", "started", "step_done").
#' @param details Named list; event-specific details.
#' @return Invisible TRUE.
#' @keywords internal
.audit_log <- function(job_id, event, details = list()) {
  entry <- list(
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
    job_id = job_id,
    event = event,
    pid = Sys.getpid()
  )
  entry <- c(entry, details)

  line <- jsonlite::toJSON(entry, auto_unbox = TRUE)

  # Per-job audit
  home <- .dsjobs_home(must_exist = FALSE)
  if (!is.null(home)) {
    job_audit <- file.path(home, "queue", job_id, "audit.jsonl")
    tryCatch(
      cat(line, "\n", file = job_audit, append = TRUE, sep = ""),
      error = function(e) NULL
    )

    # Global audit (monthly rotation)
    audit_dir <- file.path(home, "audit")
    dir.create(audit_dir, recursive = TRUE, showWarnings = FALSE)
    global_audit <- file.path(audit_dir,
                               paste0("audit_", format(Sys.time(), "%Y-%m"), ".jsonl"))
    tryCatch(
      cat(line, "\n", file = global_audit, append = TRUE, sep = ""),
      error = function(e) NULL
    )
  }

  invisible(TRUE)
}
