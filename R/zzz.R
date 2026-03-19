# Module: Package Hooks + Environments
# Package load/detach hooks, internal environments, and stale job cleanup.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment for active job handles and worker tracking
.dsjobs_env <- new.env(parent = emptyenv())

#' Package attach hook
#' @param lib Library path.
#' @param pkg Package name.
#' @keywords internal
.onAttach <- function(lib, pkg) {
  packageStartupMessage("dsJobs v", utils::packageVersion("dsJobs"), " loaded.")
  .cleanup_stale_jobs()
}

#' Remove stale jobs older than 48 hours that are still RUNNING
#'
#' On attach, scans DSJOBS_HOME for jobs stuck in RUNNING state whose
#' processx workers are dead. Transitions them to FAILED.
#'
#' @param max_age_hours Numeric; max hours before a running job is stale.
#' @keywords internal
.cleanup_stale_jobs <- function(max_age_hours = 48) {
  home <- .dsjobs_home(must_exist = FALSE)
  if (is.null(home) || !dir.exists(home)) return(invisible(NULL))

  queue_dir <- file.path(home, "queue")
  if (!dir.exists(queue_dir)) return(invisible(NULL))

  job_dirs <- list.dirs(queue_dir, full.names = TRUE, recursive = FALSE)
  for (d in job_dirs) {
    tryCatch({
      state <- .store_read_state(basename(d))
      if (is.null(state)) next
      if (!identical(state$state, "RUNNING")) next

      info <- file.info(file.path(d, "state.json"))
      if (is.na(info$mtime)) next
      age_hours <- as.numeric(difftime(Sys.time(), info$mtime, units = "hours"))
      if (age_hours > max_age_hours) {
        .store_update_state(basename(d), list(
          state = "FAILED",
          error = "Stale job cleaned up after timeout",
          finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
        ))
        .audit_log(basename(d), "cleanup", list(reason = "stale_timeout"))
      }
    }, error = function(e) NULL)
  }
}

#' Package detach hook
#'
#' Kills all active job workers on package unload.
#'
#' @param lib Library path.
#' @return Invisible NULL.
#' @keywords internal
.onDetach <- function(lib) {
  workers <- ls(.dsjobs_env, pattern = "^worker_")
  for (wk in workers) {
    tryCatch({
      proc <- get(wk, envir = .dsjobs_env)
      if (!is.null(proc) && inherits(proc, "process") && proc$is_alive()) {
        proc$signal(15L)
        proc$wait(timeout = 5000)
        if (proc$is_alive()) proc$kill()
      }
    }, error = function(e) NULL)
  }
  rm(list = ls(.dsjobs_env), envir = .dsjobs_env)
}

#' Get DSJOBS_HOME path
#'
#' @param must_exist Logical; if TRUE, stop if directory doesn't exist.
#' @return Character; path to DSJOBS_HOME.
#' @keywords internal
.dsjobs_home <- function(must_exist = TRUE) {
  home <- .dsj_option("home", "/var/lib/dsjobs")
  if (must_exist && !dir.exists(home)) {
    stop("DSJOBS_HOME does not exist: ", home,
         ". Set dsjobs.home option or create the directory.", call. = FALSE)
  }
  home
}

#' Read a dsJobs option with DataSHIELD double-fallback
#'
#' Option chain: dsjobs.{name} -> default.dsjobs.{name} -> default.
#'
#' @param name Character; the option name (without the dsjobs. prefix).
#' @param default The fallback value.
#' @return The option value.
#' @keywords internal
.dsj_option <- function(name, default = NULL) {
  getOption(
    paste0("dsjobs.", name),
    getOption(paste0("default.dsjobs.", name), default)
  )
}

#' Deserialize a possibly-JSON argument (Opal transport)
#'
#' @param x An argument that may be a B64/JSON string or native R object.
#' @return The deserialized R object.
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
