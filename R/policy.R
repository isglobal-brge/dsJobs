# Module: Trust Profiles and Disclosure Controls
# Delegates to dsFlower if available, standalone fallback otherwise.

#' @keywords internal
.dsjobs_trust_profile <- function() {
  if (requireNamespace("dsFlower", quietly = TRUE)) {
    tryCatch(return(dsFlower:::.flowerTrustProfile()), error = function(e) NULL)
  }
  profile_name <- .dsj_option("privacy_profile", "secure")
  profiles <- list(
    research = list(name = "research", min_train_rows = 3),
    secure = list(name = "secure", min_train_rows = 100),
    secure_dp = list(name = "secure_dp", min_train_rows = 200)
  )
  if (!profile_name %in% names(profiles)) {
    stop("Unknown privacy profile: '", profile_name, "'.", call. = FALSE)
  }
  profiles[[profile_name]]
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
