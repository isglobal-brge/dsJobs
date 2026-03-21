# Module: Package Hooks + Core Utilities
# No daemon start here. Worker is external (systemd/Docker).

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment (plugin registries, cached connections)
.dsjobs_env <- new.env(parent = emptyenv())

# Publisher plugin registry
.dsjobs_env$.publishers <- list()

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Ensure DSJOBS_HOME exists with required subdirectories.
  # The configure script should create these during R CMD INSTALL,
  # but Opal/Rock API installs may skip configure scripts entirely.
  # This fallback creates the structure at package load time.
  home <- .dsj_option("home", "/srv/dsjobs")
  subdirs <- c("artifacts", "runners", "publish")
  for (d in c(home, file.path(home, subdirs))) {
    if (!dir.exists(d)) {
      tryCatch(
        dir.create(d, recursive = TRUE, showWarnings = FALSE, mode = "0750"),
        error = function(e) NULL
      )
    }
  }
  invisible(NULL)
}

# --- Core utilities ---

#' @keywords internal
.dsjobs_home <- function(must_exist = TRUE) {
  home <- .dsj_option("home", "/srv/dsjobs")
  if (must_exist && !dir.exists(home)) {
    stop("DSJOBS_HOME does not exist: ", home, call. = FALSE)
  }
  home
}

#' @keywords internal
.dsj_option <- function(name, default = NULL) {
  getOption(paste0("dsjobs.", name),
    getOption(paste0("default.dsjobs.", name), default))
}

#' Deserialize B64/JSON argument from Opal transport
#' @keywords internal
.ds_arg <- function(x) {
  if (is.character(x) && length(x) == 1) {
    if (startsWith(x, "B64:")) {
      b64 <- substring(x, 5)
      b64 <- gsub("-", "+", b64)
      b64 <- gsub("_", "/", b64)
      pad <- (4 - nchar(b64) %% 4) %% 4
      if (pad > 0) b64 <- paste0(b64, strrep("=", pad))
      json <- rawToChar(jsonlite::base64_dec(b64))
      return(jsonlite::fromJSON(json, simplifyVector = FALSE))
    }
    if (nchar(x) > 0 && substr(x, 1, 1) %in% c("{", "[")) {
      return(jsonlite::fromJSON(x, simplifyVector = FALSE))
    }
  }
  x
}

#' Get owner identity
#'
#' In Opal/Rock, the DataSHIELD session user identity is NOT available
#' in the R process environment (Rock authenticates with its own internal
#' user, not the Opal user). Therefore, the client must pass the owner_id
#' as part of the job spec (.owner field). This function provides fallbacks
#' for DSLite and local testing.
#'
#' @param spec_owner Character or NULL; owner from the job spec (.owner field).
#' @keywords internal
.get_owner_id <- function(spec_owner = NULL) {
  # Best: explicit owner from client (injected by dsJobsClient)
  if (!is.null(spec_owner) && nzchar(spec_owner)) return(spec_owner)
  # DSLite / local fallback
  owner <- Sys.getenv("USER", unset = "")
  if (nzchar(owner)) return(owner)
  "anonymous"
}

#' Validate path-safe identifier
#' @keywords internal
.validate_identifier <- function(x, field_name) {
  if (!is.character(x) || length(x) != 1 || !nzchar(x))
    stop(field_name, " must be a non-empty string.", call. = FALSE)
  if (grepl("\\.\\.", x))
    stop(field_name, " must not contain '..'.", call. = FALSE)
  if (!grepl("^[a-zA-Z0-9][a-zA-Z0-9_.-]*$", x))
    stop(field_name, " contains invalid characters.", call. = FALSE)
  x
}

#' Generate unique job ID (UUIDv4, 122 bits entropy)
#' @keywords internal
.generate_job_id <- function() {
  paste0("job_", uuid::UUIDgenerate())
}

#' Check if PID is alive
#'
#' Uses /proc filesystem (Linux) as primary check, falls back to
#' kill(pid, 0) signal. The /proc check is more reliable in containers
#' where tools::pskill can return success for zombie/dead PIDs.
#'
#' @keywords internal
.pid_is_alive <- function(pid) {
  if (is.null(pid) || is.na(pid)) return(FALSE)
  pid <- as.integer(pid)

  # Primary: check /proc/<pid>/status (Linux containers)
  proc_path <- paste0("/proc/", pid, "/status")
  if (file.exists(proc_path)) {
    # Process exists -- but check if zombie
    status_lines <- tryCatch(readLines(proc_path, warn = FALSE),
                              error = function(e) character(0))
    state_line <- grep("^State:", status_lines, value = TRUE)
    if (length(state_line) > 0 && grepl("Z \\(zombie\\)", state_line[1]))
      return(FALSE)
    return(TRUE)
  }

  # /proc not available or PID not there -- process is dead
  if (dir.exists("/proc")) return(FALSE)

  # Non-Linux fallback: use kill signal 0
  tryCatch({ tools::pskill(pid, signal = 0L); TRUE },
           error = function(e) FALSE)
}
