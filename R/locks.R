# Module: File-Based Locking
# Reuses the file-based locking pattern from dsFlower/R/python_env.R.

#' Acquire a file-based lock
#'
#' @param lock_path Character; path to the lock file.
#' @param timeout_secs Numeric; max seconds to wait.
#' @param stale_mins Numeric; minutes after which a lock is stale.
#' @return TRUE if acquired.
#' @keywords internal
.lock_acquire <- function(lock_path, timeout_secs = 60, stale_mins = 15) {
  dir.create(dirname(lock_path), recursive = TRUE, showWarnings = FALSE)
  deadline <- Sys.time() + timeout_secs

  repeat {
    if (!file.exists(lock_path)) {
      tryCatch({
        con <- file(lock_path, open = "wx")
        writeLines(as.character(Sys.getpid()), con)
        close(con)
        return(TRUE)
      }, error = function(e) {})
    }

    # Check for stale lock
    lock_age <- difftime(Sys.time(), file.info(lock_path)$mtime, units = "mins")
    if (!is.na(lock_age) && lock_age > stale_mins) {
      unlink(lock_path)
      next
    }

    if (Sys.time() > deadline) {
      stop("Timeout acquiring lock: ", lock_path, call. = FALSE)
    }

    Sys.sleep(1)
  }
}

#' Release a file-based lock
#'
#' @param lock_path Character; path to the lock file.
#' @return Invisible TRUE.
#' @keywords internal
.lock_release <- function(lock_path) {
  if (file.exists(lock_path)) {
    unlink(lock_path)
  }
  invisible(TRUE)
}

#' Acquire a dataset-level lock
#'
#' @param dataset_id Character; the dataset identifier.
#' @param timeout_secs Numeric; max seconds to wait.
#' @return The lock path (for release).
#' @keywords internal
.lock_dataset <- function(dataset_id, timeout_secs = 60) {
  home <- .dsjobs_home()
  lock_dir <- file.path(home, "locks")
  dir.create(lock_dir, recursive = TRUE, showWarnings = FALSE)
  lock_path <- file.path(lock_dir, paste0("dataset.", dataset_id, ".lock"))
  .lock_acquire(lock_path, timeout_secs = timeout_secs)
  lock_path
}

#' Acquire the global lock
#'
#' @param timeout_secs Numeric; max seconds to wait.
#' @return The lock path (for release).
#' @keywords internal
.lock_global <- function(timeout_secs = 30) {
  home <- .dsjobs_home()
  lock_dir <- file.path(home, "locks")
  dir.create(lock_dir, recursive = TRUE, showWarnings = FALSE)
  lock_path <- file.path(lock_dir, "global.lock")
  .lock_acquire(lock_path, timeout_secs = timeout_secs)
  lock_path
}
