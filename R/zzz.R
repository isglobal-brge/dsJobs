# Module: Package Hooks + Core Utilities
# No daemon start here. Worker is external (systemd/Docker).

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment (plugin registries, cached connections)
.dsjobs_env <- new.env(parent = emptyenv())

# Publisher plugin registry
.dsjobs_env$.publishers <- list()

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Validate config only. Never start daemons.
  invisible(NULL)
}

# --- Core utilities ---

#' @keywords internal
.dsjobs_home <- function(must_exist = TRUE) {
  home <- .dsj_option("home", "/var/lib/dsjobs")
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

#' Generate a high-entropy access token (256 bits hex)
#' @keywords internal
.generate_access_token <- function() {
  paste(sample(c(0:9, letters[1:6]), 64, replace = TRUE), collapse = "")
}

#' Hash an access token for storage (SHA-256)
#' @keywords internal
.hash_token <- function(token) {
  digest::digest(token, algo = "sha256", serialize = FALSE)
}

#' Check if PID is alive
#' @keywords internal
.pid_is_alive <- function(pid) {
  if (is.null(pid) || is.na(pid)) return(FALSE)
  tryCatch({ tools::pskill(pid, signal = 0L); TRUE },
           error = function(e) FALSE)
}
