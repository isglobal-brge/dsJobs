# Module: Step Execution
# Called by worker daemon only. Never by DS methods.

.BLOCKED_ENV_VARS <- c("PATH", "HOME", "USER", "SHELL",
  "LD_PRELOAD", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH",
  "DYLD_INSERT_LIBRARIES", "PYTHONPATH", "PYTHONSTARTUP",
  "BASH_ENV", "ENV", "CDPATH", "IFS")

#' Execute the current step of a job
#' @keywords internal
.executor_run_step <- function(db, job_id, step_index, spec) {
  step <- spec$steps[[step_index]]
  step_dir <- .ensure_step_dir(job_id, step_index)
  input_dir <- .resolve_step_input(db, job_id, step_index, step)

  .store_update_step(db, job_id, step_index, state = "running",
    started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))

  if (identical(step$plane, "session")) {
    .run_session_step(db, job_id, step_index, step, step_dir, input_dir)
  } else {
    .run_artifact_step(db, job_id, step_index, step, step_dir, input_dir)
  }
}

#' Advance after step success
#' @keywords internal
.executor_advance <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (is.null(job)) return()
  current <- as.integer(job$step_index)
  total <- as.integer(job$total_steps)

  if (current >= total) {
    # All done -- build safe result
    .build_job_result(db, job_id)
    .store_update_job(db, job_id, state = "FINISHED", worker_pid = NA_integer_,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"))
    .db_log_event(db, job_id, "finished")
    return()
  }

  next_idx <- current + 1L
  .store_update_job(db, job_id, step_index = next_idx)
  spec <- .store_get_spec(db, job_id)
  .executor_run_step(db, job_id, next_idx, spec)
}

#' Kill a worker process
#' @keywords internal
.executor_kill <- function(db, job_id) {
  job <- .store_get_job(db, job_id)
  if (!is.null(job) && !is.na(job$worker_pid)) {
    pid <- as.integer(job$worker_pid)
    if (.pid_is_alive(pid)) {
      tools::pskill(pid, signal = 15L)
      Sys.sleep(2)
      if (.pid_is_alive(pid)) tools::pskill(pid, signal = 9L)
    }
    .store_update_job(db, job_id, worker_pid = NA_integer_)
  }
}

#' Build the final result object for a completed job
#'
#' Collects outputs from the outputs table and writes result.rds.
#' Separates client-safe outputs (returned via aggregate) from
#' server-only outputs (loadable via assign).
#'
#' @keywords internal
.build_job_result <- function(db, job_id) {
  home <- .dsjobs_home()
  result_dir <- file.path(home, "artifacts", job_id, "result")
  dir.create(result_dir, recursive = TRUE, showWarnings = FALSE)

  trust <- .dsjobs_trust_profile()

  # Client-safe outputs (returned via jobResultDS aggregate)
  safe_outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref, size_bytes FROM outputs
     WHERE job_id = ? AND safe_for_client = 1",
    params = list(job_id))

  safe_result <- list(job_id = job_id, profile = trust$name)

  if (nrow(safe_outputs) > 0) {
    safe_result$outputs <- lapply(seq_len(nrow(safe_outputs)), function(i) {
      row <- safe_outputs[i, ]
      out <- list(name = row$name, kind = row$kind)
      # Load RDS value if it's a small safe output
      if (!is.na(row$path_or_ref) && file.exists(row$path_or_ref) &&
          grepl("\\.rds$", row$path_or_ref)) {
        out$value <- readRDS(row$path_or_ref)
      }
      out
    })
  }

  # All outputs (for server-side loading via jobLoadOutputDS assign)
  all_outputs <- DBI::dbGetQuery(db,
    "SELECT name, kind, path_or_ref FROM outputs WHERE job_id = ?",
    params = list(job_id))

  safe_result$available_outputs <- if (nrow(all_outputs) > 0) {
    lapply(seq_len(nrow(all_outputs)), function(i) {
      list(name = all_outputs$name[i], kind = all_outputs$kind[i])
    })
  } else list()

  safe_result$ready <- TRUE

  saveRDS(safe_result, file.path(result_dir, "result.rds"))
  safe_result
}

# --- Helpers ---

#' @keywords internal
.ensure_step_dir <- function(job_id, step_index) {
  home <- .dsjobs_home()
  step_dir <- file.path(home, "artifacts", job_id,
                         sprintf("step_%03d", step_index))
  dir.create(file.path(step_dir, "output"), recursive = TRUE, showWarnings = FALSE)
  Sys.chmod(step_dir, "0700")
  step_dir
}

#' @keywords internal
.resolve_step_input <- function(db, job_id, step_index, step_spec) {
  # Explicit input ref
  if (!is.null(step_spec$inputs)) {
    refs <- step_spec$inputs
    # Take first ref for now (future: multiple inputs)
    if (is.list(refs) && length(refs) > 0) {
      ref <- refs[[1]]
      if (is.numeric(ref)) {
        row <- DBI::dbGetQuery(db,
          "SELECT output_ref FROM steps WHERE job_id = ? AND step_index = ?",
          params = list(job_id, as.integer(ref)))
        if (nrow(row) > 0 && !is.na(row$output_ref[1]))
          return(file.path(.dsjobs_home(), row$output_ref[1]))
      }
    }
  }
  # Default: previous step output
  if (step_index > 1L) {
    row <- DBI::dbGetQuery(db,
      "SELECT output_ref FROM steps WHERE job_id = ? AND step_index = ?",
      params = list(job_id, step_index - 1L))
    if (nrow(row) > 0 && !is.na(row$output_ref[1]))
      return(file.path(.dsjobs_home(), row$output_ref[1]))
  }
  NULL
}
