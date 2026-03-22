# Module: Session-Plane Runners
# Synchronous in-process steps. Registers outputs in DB.

#' @keywords internal
.run_session_step <- function(db, job_id, step_index, step, step_dir, input_dir) {
  result <- tryCatch({
    switch(step$type,
      resolve_dataset = .session_resolve_dataset(step, step_dir, db, job_id, step_index),
      assign_table    = list(type = "assign_table", symbol = step$symbol),
      assign_resource = list(type = "assign_resource", symbol = step$symbol),
      assign_expr     = list(type = "assign_expr", symbol = step$symbol),
      aggregate       = list(type = "aggregate", expr = step$expr),
      emit            = .session_emit(step, step_dir, db, job_id, step_index),
      safe_summary    = .session_safe_summary(job_id, step_dir, input_dir, db, step_index),
      publish_asset   = .execute_publish(job_id, step, input_dir, db),
      publish_dataset = .execute_publish(job_id, step, input_dir, db),
      stop("Unknown session step type: ", step$type, call. = FALSE)
    )
  }, error = function(e) {
    .store_update_step(db, job_id, step_index, state = "failed",
      error_message = conditionMessage(e),
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .store_update_job(db, job_id, state = "FAILED",
      error_message = conditionMessage(e), worker_pid = NA_integer_,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "step_failed",
      list(step_index = step_index, error = conditionMessage(e)))
    return(NULL)
  })

  if (!is.null(result)) {
    output_ref <- file.path("artifacts", job_id,
                             sprintf("step_%03d", step_index), "output")
    .store_update_step(db, job_id, step_index, state = "done",
      output_ref = output_ref,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "step_done", list(step_index = step_index))
    .executor_advance(db, job_id)
  }
}

#' @keywords internal
.session_resolve_dataset <- function(step, step_dir, db, job_id, step_index) {
  dataset_id <- step$dataset_id
  if (is.null(dataset_id)) stop("resolve_dataset requires 'dataset_id'.", call. = FALSE)
  if (!requireNamespace("dsImaging", quietly = TRUE))
    stop("dsImaging package required for dataset resolution.", call. = FALSE)
  manifest_path <- dsImaging::resolve_dataset(dataset_id)
  # Save result as loadable output
  out <- list(dataset_id = dataset_id, manifest_path = manifest_path)
  out_path <- file.path(step_dir, "output", "resolved.rds")
  saveRDS(out, out_path)
  .db_register_output(db, job_id, step_index, "resolved_dataset",
    "dataset_ref", out_path, safe_for_client = FALSE)
  out
}

#' @keywords internal
.session_emit <- function(step, step_dir, db, job_id, step_index) {
  output_name <- step$output_name %||% "output"
  value <- step$value
  out_path <- file.path(step_dir, "output", paste0(output_name, ".rds"))
  saveRDS(value, out_path)
  size <- file.info(out_path)$size

  # Emit outputs stay server-side -- NOT safe for client disclosure.
  # Use jobLoadOutputDS (assign) to load them into the server R session.
  .db_register_output(db, job_id, step_index, output_name,
    "emit_value", out_path, size_bytes = size, safe_for_client = FALSE)

  list(type = "emit", name = output_name, path = out_path)
}

#' @keywords internal
.session_safe_summary <- function(job_id, step_dir, input_dir, db, step_index) {
  summary <- list(job_id = job_id)

  if (!is.null(input_dir) && dir.exists(input_dir)) {
    files <- list.files(input_dir, full.names = TRUE)
    summary$n_output_files <- length(files)
    summary$output_size_bytes <- sum(file.info(files)$size, na.rm = TRUE)

    # Count rows in output tables for summary reporting.
    # nfilter enforcement happens at upstream layers, not here.
    # The summary only reports
    # bucketed counts -- the data already exists on the server.
    for (f in files) {
      if (grepl("\\.csv$", f, ignore.case = TRUE)) {
        summary$n_samples <- length(readLines(f, warn = FALSE)) - 1L
      } else if (grepl("\\.parquet$", f, ignore.case = TRUE)) {
        summary$n_samples <- nrow(arrow::read_parquet(f, as_data_frame = FALSE))
      }
    }
  }

  # Bucket counts (standard DataSHIELD disclosure control)
  if (!is.null(summary$n_output_files)) {
    n <- as.integer(summary$n_output_files)
    if (!is.na(n) && n >= 4)
      summary$n_output_files <- as.integer(2^round(log2(n)))
  }
  if (!is.null(summary$n_samples)) {
    n <- as.integer(summary$n_samples)
    if (!is.na(n) && n >= 4)
      summary$n_samples <- as.integer(2^round(log2(n)))
  }

  out_path <- file.path(step_dir, "output", "summary.rds")
  saveRDS(summary, out_path)
  .db_register_output(db, job_id, step_index, "safe_summary",
    "summary", out_path, safe_for_client = TRUE)
  summary
}
