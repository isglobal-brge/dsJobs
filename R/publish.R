# Module: Output Publication
# Atomic writes of job results to dsImaging registry and DSJOBS_HOME/publish.

#' Publish a dataset asset from job output
#'
#' Atomically writes output to DSJOBS_HOME/publish/<dataset_id>/<asset_name>/,
#' then updates the dsImaging registry.
#'
#' @param job_id Character; the job ID.
#' @param dataset_id Character; target dataset identifier.
#' @param asset_name Character; name for the published asset.
#' @param asset_type Character; type of asset (e.g. "radiomics", "preprocessed").
#' @param source_dir Character; directory containing files to publish.
#' @return Named list with publish path and status.
#' @keywords internal
.publish_dataset_asset <- function(job_id, dataset_id, asset_name,
                                    asset_type, source_dir) {
  home <- .dsjobs_home()
  lock_path <- .lock_dataset(dataset_id)

  tryCatch({
    # Stage to temp directory first
    staging_dir <- file.path(home, "publish", "staging",
                              paste0(job_id, "_", asset_name))
    dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)

    # Copy output files to staging
    files <- list.files(source_dir, full.names = TRUE)
    for (f in files) {
      file.copy(f, staging_dir, recursive = TRUE)
    }

    # Atomic rename to final location
    publish_dir <- file.path(home, "publish", dataset_id, asset_name)
    dir.create(dirname(publish_dir), recursive = TRUE, showWarnings = FALSE)

    if (dir.exists(publish_dir)) {
      # Back up existing
      backup_dir <- paste0(publish_dir, ".bak.", format(Sys.time(), "%Y%m%d%H%M%S"))
      file.rename(publish_dir, backup_dir)
    }

    file.rename(staging_dir, publish_dir)

    # Update dsImaging registry if available
    .update_imaging_registry(dataset_id, asset_name, asset_type, publish_dir)

    .audit_log(job_id, "asset_published", list(
      dataset_id = dataset_id, asset_name = asset_name,
      asset_type = asset_type, path = publish_dir
    ))

    list(
      status = "published",
      dataset_id = dataset_id,
      asset_name = asset_name,
      path = publish_dir
    )
  }, finally = {
    .lock_release(lock_path)
  })
}

#' Publish a new dataset from job output
#'
#' Creates a new entry in the dsImaging registry for a fully new dataset.
#'
#' @param job_id Character; the job ID.
#' @param dataset_id Character; new dataset identifier.
#' @param title Character; human-readable dataset title.
#' @param modality Character; imaging modality.
#' @param source_dir Character; directory containing manifest and data.
#' @return Named list with status.
#' @keywords internal
.publish_new_dataset <- function(job_id, dataset_id, title, modality,
                                  source_dir) {
  home <- .dsjobs_home()
  lock_path <- .lock_global()

  tryCatch({
    publish_dir <- file.path(home, "publish", dataset_id)
    dir.create(publish_dir, recursive = TRUE, showWarnings = FALSE)

    files <- list.files(source_dir, full.names = TRUE)
    for (f in files) {
      file.copy(f, publish_dir, recursive = TRUE)
    }

    # Write a minimal manifest
    manifest <- list(
      dataset_id = dataset_id,
      title = title,
      modality = modality,
      created_by = job_id,
      created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
    )
    manifest_path <- file.path(publish_dir, "manifest.yaml")
    if (requireNamespace("yaml", quietly = TRUE)) {
      writeLines(yaml::as.yaml(manifest), manifest_path)
    }

    .audit_log(job_id, "dataset_published", list(
      dataset_id = dataset_id, title = title
    ))

    list(status = "published", dataset_id = dataset_id, path = publish_dir)
  }, finally = {
    .lock_release(lock_path)
  })
}

#' Publish a disclosure-safe result from a completed job
#'
#' Reads step outputs and constructs a safe result object.
#'
#' @param job_id Character; the job ID.
#' @param spec Named list; the job spec.
#' @return Named list; the safe result.
#' @keywords internal
.publish_safe_result <- function(job_id, spec) {
  home <- .dsjobs_home()
  result_dir <- file.path(home, "queue", job_id, "result")
  dir.create(result_dir, recursive = TRUE, showWarnings = FALSE)

  # Collect outputs from steps
  safe_result <- list(
    job_id = job_id,
    state = "PUBLISHED",
    profile = .dsjobs_trust_profile()$name
  )

  # Look for summary outputs
  steps_dir <- file.path(home, "queue", job_id, "steps")
  step_dirs <- list.dirs(steps_dir, full.names = TRUE, recursive = FALSE)

  for (sd in step_dirs) {
    summary_path <- file.path(sd, "output", "summary.rds")
    if (file.exists(summary_path)) {
      summary <- readRDS(summary_path)
      safe_result$summary <- summary
    }
  }

  # Handle publish spec
  if (!is.null(spec$publish)) {
    pub <- spec$publish
    if (!is.null(pub$dataset_id) && !is.null(pub$asset_name)) {
      last_step_dir <- step_dirs[length(step_dirs)]
      output_dir <- file.path(last_step_dir, "output")
      if (dir.exists(output_dir) && length(list.files(output_dir)) > 0) {
        pub_result <- .publish_dataset_asset(
          job_id = job_id,
          dataset_id = pub$dataset_id,
          asset_name = pub$asset_name,
          asset_type = pub$asset_type %||% "derived",
          source_dir = output_dir
        )
        safe_result$published <- pub_result
      }
    }
  }

  # Save result
  result_path <- file.path(result_dir, "result.rds")
  saveRDS(safe_result, result_path)

  safe_result
}

#' Update dsImaging registry with a new asset entry
#'
#' @param dataset_id Character; dataset identifier.
#' @param asset_name Character; asset name.
#' @param asset_type Character; asset type.
#' @param asset_path Character; path to the published asset.
#' @keywords internal
.update_imaging_registry <- function(dataset_id, asset_name,
                                      asset_type, asset_path) {
  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    return(invisible(NULL))
  }

  registry_path <- getOption("dsimaging.registry_path",
                              getOption("default.dsimaging.registry_path", NULL))
  if (is.null(registry_path) || !file.exists(registry_path)) {
    return(invisible(NULL))
  }

  if (!requireNamespace("yaml", quietly = TRUE)) {
    return(invisible(NULL))
  }

  # Atomic read-modify-write
  registry <- yaml::read_yaml(registry_path)

  if (is.null(registry[[dataset_id]])) {
    return(invisible(NULL))
  }

  # Add or update assets section
  if (is.null(registry[[dataset_id]]$assets)) {
    registry[[dataset_id]]$assets <- list()
  }

  registry[[dataset_id]]$assets[[asset_name]] <- list(
    type = asset_type,
    root = asset_path,
    added_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )

  # Write atomically
  tmp_path <- paste0(registry_path, ".tmp")
  writeLines(yaml::as.yaml(registry), tmp_path)
  file.rename(tmp_path, registry_path)

  invisible(TRUE)
}
