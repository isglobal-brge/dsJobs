# Module: Disclosure Controls
# Standard DataSHIELD disclosure settings for dsJobs.

#' Read a dsJobs option with DataSHIELD double-fallback
#'
#' Option chain: dsjobs.{name} -> default.dsjobs.{name} -> default.
#' @keywords internal
.dsj_disclosure_settings <- function() {
  list(
    nfilter_subset = as.numeric(
      getOption("nfilter.subset", getOption("default.nfilter.subset", 3))
    )
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
