# Module: Plugin Registry
# Generic extension points for publishers, dataset adapters, runners.
# Domain packages (dsImaging, dsRadiomics) register their hooks here.

#' Register a publisher plugin
#'
#' Called by domain packages (dsImaging, dsRadiomics) to register
#' publish logic. dsJobs calls the registered function when a
#' publish_asset or publish_dataset step completes.
#'
#' @param kind Character; publisher kind (e.g. "imaging_asset", "radiomics_dataset").
#' @param fn Function; publisher function(job_id, step, output_dir, db) -> list.
#' @export
register_dsjobs_publisher <- function(kind, fn) {
  if (!is.function(fn)) stop("Publisher must be a function.", call. = FALSE)
  .dsjobs_env$.publishers[[kind]] <- fn
  invisible(TRUE)
}

#' Get a registered publisher
#' @keywords internal
.get_publisher <- function(kind) {
  .dsjobs_env$.publishers[[kind]]
}

#' List registered publishers
#' @keywords internal
.list_publishers <- function() {
  names(.dsjobs_env$.publishers)
}

#' Execute a publish step via plugin or fallback
#'
#' If a domain-specific publisher is registered for the step's
#' publish_kind, delegates to it. Otherwise uses generic filesystem copy.
#'
#' @keywords internal
.execute_publish <- function(job_id, step, output_dir, db) {
  publish_kind <- step$publish_kind %||% "generic"
  publisher <- .get_publisher(publish_kind)

  if (!is.null(publisher)) {
    # Delegate to domain-specific publisher
    return(publisher(job_id, step, output_dir, db))
  }

  # Generic fallback: copy output to publish dir
  .publish_generic(job_id, step, output_dir, db)
}

#' Generic filesystem publisher
#' @keywords internal
.publish_generic <- function(job_id, step, output_dir, db) {
  dataset_id <- step$dataset_id
  asset_name <- step$asset_name
  if (is.null(dataset_id) || is.null(asset_name)) {
    return(list(status = "skipped", reason = "no dataset_id or asset_name"))
  }

  .validate_identifier(dataset_id, "dataset_id")
  .validate_identifier(asset_name, "asset_name")

  home <- .dsjobs_home()
  lock_path <- .lock_acquire_dataset(dataset_id)
  tryCatch({
    publish_dir <- file.path(home, "publish", dataset_id, asset_name)
    dir.create(dirname(publish_dir), recursive = TRUE, showWarnings = FALSE)
    if (dir.exists(publish_dir)) {
      backup <- paste0(publish_dir, ".bak.", format(Sys.time(), "%Y%m%d%H%M%S"))
      file.rename(publish_dir, backup)
    }
    # Copy output to publish location
    dir.create(publish_dir, recursive = TRUE, showWarnings = FALSE)
    files <- list.files(output_dir, full.names = TRUE)
    for (f in files) file.copy(f, publish_dir, recursive = TRUE)

    .db_log_event(db, job_id, "published",
      list(dataset_id = dataset_id, asset_name = asset_name))
    .db_register_output(db, job_id, step$step_index %||% NA_integer_,
      asset_name, "published_asset", publish_dir, safe_for_client = FALSE)

    list(status = "published", dataset_id = dataset_id, asset_name = asset_name,
         path = publish_dir)
  }, finally = .lock_release(lock_path))
}

# --- File locks (for publish only, not for DB operations) ---

#' @keywords internal
.lock_acquire_dataset <- function(dataset_id, timeout_secs = 60) {
  home <- .dsjobs_home()
  lock_dir <- file.path(home, "locks")
  dir.create(lock_dir, recursive = TRUE, showWarnings = FALSE)
  lock_path <- file.path(lock_dir, paste0("dataset.", dataset_id, ".lock"))
  .lock_acquire(lock_path, timeout_secs)
  lock_path
}

#' @keywords internal
.lock_acquire <- function(lock_path, timeout_secs = 60, stale_mins = 15) {
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
    lock_age <- difftime(Sys.time(), file.info(lock_path)$mtime, units = "mins")
    if (!is.na(lock_age) && lock_age > stale_mins) { unlink(lock_path); next }
    if (Sys.time() > deadline) stop("Timeout acquiring lock: ", lock_path, call. = FALSE)
    Sys.sleep(1)
  }
}

#' @keywords internal
.lock_release <- function(lock_path) {
  if (file.exists(lock_path)) unlink(lock_path)
  invisible(TRUE)
}
