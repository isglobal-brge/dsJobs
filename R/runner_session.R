# Module: Session-Plane Runners
# Synchronous, in-process steps. After completion, advances immediately.

#' @keywords internal
.run_session_step <- function(db, job_id, step_index, step, step_dir, input_dir) {
  result <- tryCatch({
    switch(step$type,
      resolve_dataset = .session_resolve_dataset(step),
      assign_table    = .session_assign_table(step),
      assign_resource = .session_assign_resource(step),
      assign_expr     = .session_assign_expr(step),
      aggregate       = .session_aggregate(step),
      emit            = .session_emit(step, step_dir),
      safe_summary    = .session_safe_summary(job_id, step, step_dir, input_dir),
      publish_asset   = .session_publish_asset(job_id, step, input_dir, db),
      publish_dataset = .session_publish_dataset(job_id, step, input_dir, db),
      stop("Unknown session step type: ", step$type, call. = FALSE)
    )
  }, error = function(e) {
    .store_update_step(db, job_id, step_index,
      state = "failed", error = conditionMessage(e),
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .store_update_job(db, job_id,
      state = "FAILED", error = conditionMessage(e),
      worker_pid = NA_integer_,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "step_failed",
      list(step_index = step_index, error = conditionMessage(e)))
    return(NULL)
  })

  if (!is.null(result)) {
    output_ref <- file.path("artifacts", job_id,
                             sprintf("step_%03d", step_index), "output")
    .store_update_step(db, job_id, step_index,
      state = "done", output_ref = output_ref,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "step_done", list(step_index = step_index))
    # Session steps complete synchronously -- advance immediately
    .executor_advance(db, job_id)
  }
}

#' @keywords internal
.session_resolve_dataset <- function(step) {
  dataset_id <- step$dataset_id
  if (is.null(dataset_id)) stop("resolve_dataset requires 'dataset_id'.", call. = FALSE)
  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    stop("dsImaging package required for dataset resolution.", call. = FALSE)
  }
  manifest_path <- dsImaging:::resolve_dataset(dataset_id)
  list(dataset_id = dataset_id, manifest_path = manifest_path)
}

#' @keywords internal
.session_assign_table <- function(step) {
  list(type = "assign_table", symbol = step$symbol, table = step$table)
}

#' @keywords internal
.session_assign_resource <- function(step) {
  list(type = "assign_resource", symbol = step$symbol, resource = step$resource)
}

#' @keywords internal
.session_assign_expr <- function(step) {
  list(type = "assign_expr", symbol = step$symbol, expr = step$expr)
}

#' @keywords internal
.session_aggregate <- function(step) {
  list(type = "aggregate", expr = step$expr)
}

#' @keywords internal
.session_emit <- function(step, step_dir) {
  output_name <- step$output_name %||% "output"
  output_path <- file.path(step_dir, "output", paste0(output_name, ".rds"))
  saveRDS(step$value, output_path)
  list(type = "emit", output_name = output_name)
}

#' @keywords internal
.session_safe_summary <- function(job_id, step, step_dir, input_dir) {
  trust <- .dsjobs_trust_profile()
  summary <- list(job_id = job_id, profile = trust$name)

  # Read output from previous artifact step if available
  if (!is.null(input_dir) && dir.exists(input_dir)) {
    output_files <- list.files(input_dir, full.names = TRUE)
    summary$n_output_files <- length(output_files)
    summary$output_size_bytes <- sum(file.info(output_files)$size, na.rm = TRUE)
  }

  # Bucket counts under non-research profiles
  if (!identical(trust$name, "research") && !is.null(summary$n_output_files)) {
    n <- as.integer(summary$n_output_files)
    if (!is.na(n) && n >= 4) {
      summary$n_output_files <- as.integer(2^round(log2(n)))
    }
  }

  output_path <- file.path(step_dir, "output", "summary.rds")
  saveRDS(summary, output_path)
  summary
}

#' @keywords internal
.session_publish_asset <- function(job_id, step, input_dir, db) {
  if (is.null(input_dir) || !dir.exists(input_dir)) {
    stop("publish_asset: no input directory from previous step.", call. = FALSE)
  }
  .publish_dataset_asset(
    job_id = job_id,
    dataset_id = step$dataset_id,
    asset_name = step$asset_name,
    asset_type = step$asset_type %||% "derived",
    source_dir = input_dir,
    db = db
  )
}

#' @keywords internal
.session_publish_dataset <- function(job_id, step, input_dir, db) {
  if (is.null(input_dir) || !dir.exists(input_dir)) {
    stop("publish_dataset: no input directory from previous step.", call. = FALSE)
  }
  .publish_new_dataset(
    job_id = job_id,
    dataset_id = step$dataset_id,
    title = step$title,
    modality = step$modality,
    source_dir = input_dir,
    db = db
  )
}
