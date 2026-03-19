# Module: Disclosure Controls
# dsJobs disclosure settings, read from server-side R options.

#' @keywords internal
.dsjobs_trust_profile <- function() {
  list(
    name                     = .dsj_option("privacy_profile", "default"),
    min_train_rows           = as.numeric(.dsj_option("min_train_rows", 100)),
    allow_exact_num_examples = as.logical(.dsj_option("allow_exact_num_examples", FALSE))
  )
}

#' @keywords internal
.sanitize_job_logs <- function(lines, last_n = 50L) {
  if (is.null(lines) || length(lines) == 0) return(character(0))
  last_n <- min(as.integer(last_n), 200L)
  if (length(lines) > last_n) lines <- utils::tail(lines, last_n)
  lines <- gsub("/[a-zA-Z0-9_./-]{3,}", "<path>", lines)
  lines <- gsub("[A-Z]:\\\\[a-zA-Z0-9_.\\\\ -]{3,}", "<path>", lines)
  lines <- gsub("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b", "<ip>", lines)
  lines <- gsub("<ip>:\\d+", "<ip>:<port>", lines)
  lines <- gsub("\\bpid[= ]+\\d+", "pid=<pid>", lines, ignore.case = TRUE)
  lines
}
