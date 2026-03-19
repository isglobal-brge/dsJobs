# Module: Output Publication
# Atomic asset publishing with file locks. Generic -- no domain concepts.

#' Publish a dataset asset from job output
#' @keywords internal
.publish_dataset_asset <- function(job_id, dataset_id, asset_name,
                                    asset_type, source_dir, db) {
  .validate_identifier(dataset_id, "dataset_id")
  .validate_identifier(asset_name, "asset_name")

  home <- .dsjobs_home()
  lock_path <- .lock_dataset(dataset_id)

  tryCatch({
    # Stage
    staging_dir <- file.path(home, "publish", "staging",
                              paste0(job_id, "_", asset_name))
    dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)

    files <- list.files(source_dir, full.names = TRUE)
    for (f in files) file.copy(f, staging_dir, recursive = TRUE)

    # Atomic rename
    publish_dir <- file.path(home, "publish", dataset_id, asset_name)
    dir.create(dirname(publish_dir), recursive = TRUE, showWarnings = FALSE)
    if (dir.exists(publish_dir)) {
      backup <- paste0(publish_dir, ".bak.", format(Sys.time(), "%Y%m%d%H%M%S"))
      file.rename(publish_dir, backup)
    }
    file.rename(staging_dir, publish_dir)

    # Update dsImaging registry (if available)
    .update_imaging_registry(dataset_id, asset_name, asset_type, publish_dir)

    .db_log_event(db, job_id, "asset_published",
      list(dataset_id = dataset_id, asset_name = asset_name))

    list(status = "published", dataset_id = dataset_id, asset_name = asset_name)
  }, finally = {
    .lock_release(lock_path)
  })
}

#' Publish a new dataset
#' @keywords internal
.publish_new_dataset <- function(job_id, dataset_id, title, modality,
                                  source_dir, db) {
  .validate_identifier(dataset_id, "dataset_id")
  home <- .dsjobs_home()

  publish_dir <- file.path(home, "publish", dataset_id)
  dir.create(publish_dir, recursive = TRUE, showWarnings = FALSE)

  files <- list.files(source_dir, full.names = TRUE)
  for (f in files) file.copy(f, publish_dir, recursive = TRUE)

  if (requireNamespace("yaml", quietly = TRUE)) {
    manifest <- list(dataset_id = dataset_id, title = title,
                      modality = modality, created_by = job_id,
                      created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))
    writeLines(yaml::as.yaml(manifest),
               file.path(publish_dir, "manifest.yaml"))
  }

  .db_log_event(db, job_id, "dataset_published",
    list(dataset_id = dataset_id, title = title))

  list(status = "published", dataset_id = dataset_id)
}

#' Publish a safe result for a finished job
#' @keywords internal
.publish_safe_result <- function(job_id, spec, db) {
  home <- .dsjobs_home()
  result_dir <- file.path(home, "artifacts", job_id, "result")
  dir.create(result_dir, recursive = TRUE, showWarnings = FALSE)

  safe_result <- list(
    job_id = job_id, state = "PUBLISHED",
    profile = .dsjobs_trust_profile()$name
  )

  # Collect summaries from steps
  steps <- DBI::dbGetQuery(db,
    "SELECT step_index, output_ref FROM steps
     WHERE job_id = ? AND state = 'done' AND output_ref IS NOT NULL",
    params = list(job_id))

  for (i in seq_len(nrow(steps))) {
    summary_path <- file.path(home, steps$output_ref[i], "summary.rds")
    if (file.exists(summary_path)) {
      safe_result$summary <- readRDS(summary_path)
    }
  }

  result_path <- file.path(result_dir, "result.rds")
  saveRDS(safe_result, result_path)
  safe_result
}

#' @keywords internal
.update_imaging_registry <- function(dataset_id, asset_name,
                                      asset_type, asset_path) {
  if (!requireNamespace("dsImaging", quietly = TRUE)) return(invisible(NULL))
  if (!requireNamespace("yaml", quietly = TRUE)) return(invisible(NULL))

  registry_path <- getOption("dsimaging.registry_path",
                              getOption("default.dsimaging.registry_path", NULL))
  if (is.null(registry_path) || !file.exists(registry_path)) return(invisible(NULL))

  # Use global lock for registry writes
  lock_path <- .lock_global()
  tryCatch({
    registry <- yaml::read_yaml(registry_path)
    if (is.null(registry[[dataset_id]])) return(invisible(NULL))

    if (is.null(registry[[dataset_id]]$assets)) {
      registry[[dataset_id]]$assets <- list()
    }
    registry[[dataset_id]]$assets[[asset_name]] <- list(
      type = asset_type, root = asset_path,
      added_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"))

    tmp_path <- paste0(registry_path, ".tmp")
    writeLines(yaml::as.yaml(registry), tmp_path)
    file.rename(tmp_path, registry_path)
  }, finally = {
    .lock_release(lock_path)
  })
  invisible(TRUE)
}

#' File-based lock for publish operations
#' @keywords internal
.lock_dataset <- function(dataset_id, timeout_secs = 60) {
  home <- .dsjobs_home()
  lock_dir <- file.path(home, "locks")
  dir.create(lock_dir, recursive = TRUE, showWarnings = FALSE)
  lock_path <- file.path(lock_dir, paste0("dataset.", dataset_id, ".lock"))
  .lock_acquire(lock_path, timeout_secs)
  lock_path
}

#' @keywords internal
.lock_global <- function(timeout_secs = 30) {
  home <- .dsjobs_home()
  lock_dir <- file.path(home, "locks")
  dir.create(lock_dir, recursive = TRUE, showWarnings = FALSE)
  lock_path <- file.path(lock_dir, "global.lock")
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

#' @keywords internal
.lock_release <- function(lock_path) {
  if (file.exists(lock_path)) unlink(lock_path)
  invisible(TRUE)
}
