# Module: Trust Profiles and Disclosure Controls
# Mirrors dsFlower/R/policy.R patterns for the jobs runtime.

#' Get the effective trust profile for dsJobs
#'
#' Reads the dsjobs.privacy_profile option (default: reads from dsFlower
#' if available, otherwise "secure").
#'
#' @return Named list of trust profile settings.
#' @keywords internal
.dsjobs_trust_profile <- function() {
  # Reuse dsFlower's trust profile if available
  if (requireNamespace("dsFlower", quietly = TRUE)) {
    tryCatch(
      return(dsFlower:::.flowerTrustProfile()),
      error = function(e) NULL
    )
  }

  # Standalone fallback
  profile_name <- .dsj_option("privacy_profile", "secure")

  profiles <- list(
    research = list(
      name = "research",
      min_train_rows = 3,
      allow_per_node_metrics = TRUE
    ),
    secure = list(
      name = "secure",
      min_train_rows = 100,
      allow_per_node_metrics = FALSE
    ),
    secure_dp = list(
      name = "secure_dp",
      min_train_rows = 200,
      allow_per_node_metrics = FALSE
    )
  )

  if (!profile_name %in% names(profiles)) {
    stop("Unknown privacy profile: '", profile_name, "'.", call. = FALSE)
  }

  if (identical(profile_name, "research")) {
    allow_research <- as.logical(.dsj_option("allow_research_profile", FALSE))
    if (!isTRUE(allow_research)) {
      stop("The 'research' privacy profile requires explicit admin opt-in. ",
           "Set: options(dsjobs.allow_research_profile = TRUE).",
           call. = FALSE)
    }
  }

  profiles[[profile_name]]
}

#' Sanitize job log lines before returning through DataSHIELD
#'
#' Strips filesystem paths, IP addresses, PIDs from log output.
#'
#' @param lines Character vector of log lines.
#' @param last_n Integer; max lines to return (default 50).
#' @return Character vector of sanitized lines.
#' @keywords internal
.sanitize_job_logs <- function(lines, last_n = 50L) {
  if (is.null(lines) || length(lines) == 0) return(character(0))

  last_n <- min(as.integer(last_n), 200L)
  if (length(lines) > last_n) {
    lines <- utils::tail(lines, last_n)
  }

  # Strip filesystem paths
  lines <- gsub("/[a-zA-Z0-9_./-]{3,}", "<path>", lines)
  lines <- gsub("[A-Z]:\\\\[a-zA-Z0-9_.\\\\ -]{3,}", "<path>", lines)

  # Strip IP addresses
  lines <- gsub("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b", "<ip>", lines)
  lines <- gsub("<ip>:\\d+", "<ip>:<port>", lines)

  # Strip PIDs
  lines <- gsub("\\bpid[= ]+\\d+", "pid=<pid>", lines, ignore.case = TRUE)

  lines
}
