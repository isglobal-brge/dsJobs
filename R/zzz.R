# Module: Package Hooks + Environments
# Package load/detach hooks, internal environments, owner resolution.

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment for worker tracking
.dsjobs_env <- new.env(parent = emptyenv())

#' @keywords internal
.onAttach <- function(lib, pkg) {
  packageStartupMessage("dsJobs v", utils::packageVersion("dsJobs"), " loaded.")
}

#' @keywords internal
.onDetach <- function(lib) {
  # Stop worker if tracked in this session
  tryCatch({
    proc <- .dsjobs_env$.worker
    if (!is.null(proc) && inherits(proc, "process") && proc$is_alive()) {
      proc$signal(15L)
      proc$wait(timeout = 5000)
      if (proc$is_alive()) proc$kill()
    }
  }, error = function(e) NULL)
  rm(list = ls(.dsjobs_env), envir = .dsjobs_env)
}

#' Get DSJOBS_HOME path
#' @param must_exist Logical; if TRUE, stop if missing.
#' @return Character path.
#' @keywords internal
.dsjobs_home <- function(must_exist = TRUE) {
  home <- .dsj_option("home", "/var/lib/dsjobs")
  if (must_exist && !dir.exists(home)) {
    stop("DSJOBS_HOME does not exist: ", home, call. = FALSE)
  }
  home
}

#' Read a dsJobs option with DataSHIELD double-fallback
#' @keywords internal
.dsj_option <- function(name, default = NULL) {
  getOption(paste0("dsjobs.", name),
    getOption(paste0("default.dsjobs.", name), default))
}

#' Deserialize a possibly-JSON argument (Opal transport)
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

#' Get the owner identity for the current DataSHIELD session
#' @return Character; owner identifier.
#' @keywords internal
.get_owner_id <- function() {
  # Rock exposes user via environment variable

  owner <- Sys.getenv("ROCK_USER", unset = "")
  if (nzchar(owner)) return(owner)
  # Opal may set this
  owner <- Sys.getenv("OPAL_USER", unset = "")
  if (nzchar(owner)) return(owner)
  # DSLite / local fallback
  Sys.getenv("USER", unset = "anonymous")
}

#' Validate a path-safe identifier
#' @keywords internal
.validate_identifier <- function(x, field_name) {
  if (!is.character(x) || length(x) != 1 || !nzchar(x)) {
    stop(field_name, " must be a non-empty string.", call. = FALSE)
  }
  if (grepl("\\.\\.", x)) {
    stop(field_name, " must not contain '..'.", call. = FALSE)
  }
  if (!grepl("^[a-zA-Z0-9][a-zA-Z0-9_.-]*$", x)) {
    stop(field_name, " contains invalid characters.", call. = FALSE)
  }
  x
}

#' Generate a unique job ID
#' @keywords internal
.generate_job_id <- function() {
  hex <- paste(sample(c(0:9, letters[1:6]), 12, replace = TRUE), collapse = "")
  paste0("job_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", Sys.getpid(), "_", hex)
}

#' Check if a PID is alive
#' @keywords internal
.pid_is_alive <- function(pid) {
  if (is.null(pid) || is.na(pid)) return(FALSE)
  tryCatch({
    # Signal 0 checks existence without killing
    tools::pskill(pid, signal = 0L)
    TRUE
  }, error = function(e) FALSE)
}
