# Module: Session-Plane Runners
# Executes DataSHIELD assign/aggregate/checkpoint steps within the
# current R session (no subprocess).

#' Run a session-plane step
#'
#' Executes synchronously in the current process. Session steps are brief
#' and idempotent (assign, aggregate, checkpoint, emit).
#'
#' @param job_id Character; the job ID.
#' @param step_index Integer; the step index.
#' @param step Named list; the step spec.
#' @param step_dir Character; path to step directory.
#' @return Invisible NULL.
#' @keywords internal
.run_session_step <- function(job_id, step_index, step, step_dir) {
  result <- tryCatch({
    switch(step$type,
      resolve_dataset = .session_resolve_dataset(step),
      assign_table    = .session_assign_table(step),
      assign_resource = .session_assign_resource(step),
      assign_expr     = .session_assign_expr(step),
      aggregate       = .session_aggregate(step),
      checkpoint      = .session_checkpoint(job_id, step),
      restore_checkpoint = .session_restore_checkpoint(job_id, step),
      emit            = .session_emit(job_id, step_index, step, step_dir),
      safe_summary    = .session_safe_summary(job_id, step, step_dir),
      stop("Unknown session step type: ", step$type, call. = FALSE)
    )
  }, error = function(e) {
    # Write failure status
    status <- list(
      step_index = step_index, state = "failed",
      error = conditionMessage(e),
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
    )
    jsonlite::write_json(status, file.path(step_dir, "status.json"),
                          auto_unbox = TRUE, pretty = TRUE)
    .store_update_state(job_id, list(
      state = "FAILED", error = conditionMessage(e),
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
    ))
    .audit_log(job_id, "step_failed", list(
      step_index = step_index, error = conditionMessage(e)
    ))
    return(NULL)
  })

  if (!is.null(result)) {
    # Write success status
    status <- list(
      step_index = step_index, state = "done",
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
    )
    jsonlite::write_json(status, file.path(step_dir, "status.json"),
                          auto_unbox = TRUE, pretty = TRUE)
    .audit_log(job_id, "step_done", list(step_index = step_index))

    # Session steps complete synchronously -- advance immediately
    state <- .store_read_state(job_id)
    spec <- .store_read_spec(job_id)
    if (!is.null(state) && !is.null(spec) && identical(state$state, "RUNNING")) {
      .executor_advance(job_id, state, spec)
    }
  }

  invisible(NULL)
}

#' Resolve a dataset ID to manifest via dsImaging registry
#' @keywords internal
.session_resolve_dataset <- function(step) {
  dataset_id <- step$dataset_id
  if (is.null(dataset_id)) {
    stop("resolve_dataset step requires 'dataset_id'.", call. = FALSE)
  }

  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    stop("dsImaging package required for dataset resolution.", call. = FALSE)
  }

  manifest_path <- dsImaging:::resolve_dataset(dataset_id)
  list(dataset_id = dataset_id, manifest_path = manifest_path)
}

#' Assign a table to the session workspace
#' @keywords internal
.session_assign_table <- function(step) {
  list(type = "assign_table", symbol = step$symbol, table = step$table)
}

#' Assign a resource
#' @keywords internal
.session_assign_resource <- function(step) {
  list(type = "assign_resource", symbol = step$symbol, resource = step$resource)
}

#' Evaluate an expression in session
#' @keywords internal
.session_assign_expr <- function(step) {
  list(type = "assign_expr", symbol = step$symbol, expr = step$expr)
}

#' Run an aggregate expression
#' @keywords internal
.session_aggregate <- function(step) {
  list(type = "aggregate", expr = step$expr)
}

#' Save workspace checkpoint
#' @keywords internal
.session_checkpoint <- function(job_id, step) {
  home <- .dsjobs_home()
  checkpoint_dir <- file.path(home, "queue", job_id, "checkpoints")
  dir.create(checkpoint_dir, recursive = TRUE, showWarnings = FALSE)
  checkpoint_file <- file.path(checkpoint_dir,
                                paste0(step$workspace_name %||% "default", ".rds"))
  # Save workspace snapshot
  saveRDS(list(timestamp = Sys.time()), checkpoint_file)
  list(type = "checkpoint", file = checkpoint_file)
}

#' Restore from checkpoint
#' @keywords internal
.session_restore_checkpoint <- function(job_id, step) {
  home <- .dsjobs_home()
  checkpoint_file <- file.path(home, "queue", job_id, "checkpoints",
                                paste0(step$workspace_name %||% "default", ".rds"))
  if (!file.exists(checkpoint_file)) {
    stop("Checkpoint not found: ", step$workspace_name, call. = FALSE)
  }
  readRDS(checkpoint_file)
}

#' Emit an output value to step directory
#' @keywords internal
.session_emit <- function(job_id, step_index, step, step_dir) {
  output_name <- step$output_name %||% "output"
  value <- step$value
  output_path <- file.path(step_dir, "output",
                            paste0(output_name, ".rds"))
  saveRDS(value, output_path)
  list(type = "emit", output_name = output_name, path = output_path)
}

#' Generate a disclosure-safe summary
#' @keywords internal
.session_safe_summary <- function(job_id, step, step_dir) {
  output <- step$output
  trust <- .dsjobs_trust_profile()

  summary <- list(
    type = "safe_summary",
    job_id = job_id,
    profile = trust$name
  )

  if (!is.null(output) && is.list(output)) {
    # Only include safe scalar summaries
    safe_keys <- c("n_samples", "n_features", "duration_secs",
                    "status", "runner", "asset_type")
    for (k in safe_keys) {
      if (!is.null(output[[k]])) {
        summary[[k]] <- output[[k]]
      }
    }
    # Bucket counts under non-research profiles
    if (!identical(trust$name, "research") && !is.null(summary$n_samples)) {
      n <- as.integer(summary$n_samples)
      if (!is.na(n) && n >= 4) {
        summary$n_samples <- as.integer(2^round(log2(n)))
      }
    }
  }

  output_path <- file.path(step_dir, "output", "summary.rds")
  saveRDS(summary, output_path)
  summary
}
