# Module: Worker Control Plane Backend
# The worker uses a SERVICE credential (PAT) to read inboxes and write mirrors.
# Credential read from file path, NEVER from R option in cleartext.
# This credential is ONLY for control plane ops, NOT for DS execution.

#' Create the worker's control plane connection
#'
#' Reads service credentials from file and connects to the backend.
#'
#' @return An opalr connection or NULL.
#' @keywords internal
.worker_cp_connect <- function() {
  backend_type <- Sys.getenv("DSJOBS_WORKER_BACKEND",
    unset = .dsj_option("worker_backend", "opal"))

  if (identical(backend_type, "opal")) {
    url <- Sys.getenv("DSJOBS_WORKER_BACKEND_URL",
      unset = .dsj_option("worker_backend_url", ""))
    user <- Sys.getenv("DSJOBS_WORKER_BACKEND_USER",
      unset = .dsj_option("worker_backend_user", "dsjobs-worker"))
    token_file <- Sys.getenv("DSJOBS_WORKER_TOKEN_FILE",
      unset = .dsj_option("worker_token_file", ""))

    if (!nzchar(url)) return(NULL)

    # Read token from file (never from option in cleartext)
    token <- NULL
    if (nzchar(token_file) && file.exists(token_file)) {
      token <- trimws(readLines(token_file, n = 1, warn = FALSE))
    }
    if (is.null(token) || !nzchar(token)) {
      .worker_log("WARNING: No service token found at ", token_file)
      return(NULL)
    }

    tryCatch({
      if (requireNamespace("opalr", quietly = TRUE)) {
        opalr::opal.login(username = user, password = token, url = url,
          opts = list(ssl_verifyhost = 0, ssl_verifypeer = 0))
      }
    }, error = function(e) {
      .worker_log("WARNING: Backend connect failed: ", conditionMessage(e))
      NULL
    })
  } else {
    NULL
  }
}

#' Disconnect worker's control plane connection
#' @keywords internal
.worker_cp_disconnect <- function(cp_conn) {
  if (!is.null(cp_conn)) {
    tryCatch(opalr::opal.logout(cp_conn), error = function(e) NULL)
  }
}

#' Scan all user inboxes for new submissions
#' @keywords internal
.worker_scan_inboxes <- function(cp_conn) {
  if (is.null(cp_conn)) return(list())

  submissions <- list()
  homes <- tryCatch(opalr::opal.file_ls(cp_conn, "/home"),
    error = function(e) data.frame())
  if (nrow(homes) == 0) return(list())

  for (i in seq_len(nrow(homes))) {
    user <- homes$name[i]
    inbox_path <- paste0("/home/", user, "/.dsjobs/inbox")
    files <- tryCatch(opalr::opal.file_ls(cp_conn, inbox_path),
      error = function(e) data.frame())
    if (nrow(files) == 0) next

    for (j in seq_len(nrow(files))) {
      fname <- files$name[j]
      tmp <- tempfile(fileext = ".json")
      tryCatch({
        opalr::opal.file_download(cp_conn,
          paste0(inbox_path, "/", fname), tmp)
        spec <- jsonlite::fromJSON(readLines(tmp, warn = FALSE),
          simplifyVector = FALSE)
        submissions[[length(submissions) + 1]] <- list(
          owner = user, spec = spec, inbox_file = paste0(inbox_path, "/", fname))
      }, error = function(e) NULL)
      unlink(tmp)
    }
  }
  submissions
}

#' Write job state mirror to user's Opal home
#' @keywords internal
.worker_write_mirror <- function(cp_conn, owner, job_id, state_obj) {
  if (is.null(cp_conn)) return()

  job_dir <- paste0("/home/", owner, "/.dsjobs/jobs/", job_id)
  tryCatch(opalr::opal.file_mkdir(cp_conn, job_dir), error = function(e) NULL)

  tmp <- tempfile("state_", fileext = ".json")
  on.exit(unlink(tmp))
  writeLines(jsonlite::toJSON(state_obj, auto_unbox = TRUE, pretty = TRUE), tmp)
  tryCatch(opalr::opal.file_upload(cp_conn, tmp, job_dir), error = function(e)
    .worker_log("Mirror write failed for ", job_id, ": ", conditionMessage(e)))
}

#' Write result mirror
#' @keywords internal
.worker_write_result_mirror <- function(cp_conn, owner, job_id, result_obj) {
  if (is.null(cp_conn)) return()

  job_dir <- paste0("/home/", owner, "/.dsjobs/jobs/", job_id)
  tryCatch(opalr::opal.file_mkdir(cp_conn, job_dir), error = function(e) NULL)

  tmp <- tempfile("result_", fileext = ".json")
  on.exit(unlink(tmp))
  writeLines(jsonlite::toJSON(result_obj, auto_unbox = TRUE, pretty = TRUE), tmp)
  tryCatch(opalr::opal.file_upload(cp_conn, tmp, job_dir), error = function(e) NULL)
}

#' Remove consumed inbox file
#' @keywords internal
.worker_consume_inbox <- function(cp_conn, inbox_file) {
  if (is.null(cp_conn)) return()
  tryCatch(opalr::opal.file_rm(cp_conn, inbox_file), error = function(e) NULL)
}
