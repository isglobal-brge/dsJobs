# Module: Artifact-Plane Runners
# Async processx subprocesses. Worker reaps on next poll.

# Global registry of processx handles so the worker can use proc$get_exit_status()
# instead of PID-based checking (which fails on Rosetta/cross-arch emulation).
.proc_registry <- new.env(parent = emptyenv())

#' @keywords internal
.run_artifact_step <- function(db, job_id, step_index, step, step_dir, input_dir) {
  runner_name <- step$runner
  runner_config <- .load_runner_config(runner_name)
  if (is.null(runner_config)) stop("Runner '", runner_name, "' not found.", call. = FALSE)

  command <- runner_config$command %||% "python"
  if (identical(command, "python")) {
    py <- .resolve_python_env(runner_config)
    command <- py$python
  }

  args <- .build_runner_args(runner_config, step, step_dir, input_dir)
  output_dir <- file.path(step_dir, "output")

  # processx expects named character vector: c(VAR = "value", ...)
  # "current" inherits the parent environment, but R's LD_LIBRARY_PATH
  # conflicts with Python native libs (pyarrow's libarrow). Clear it.
  env_vars <- c(
    "current",
    LD_LIBRARY_PATH = "",
    DSJOBS_STEP_DIR = step_dir,
    DSJOBS_OUTPUT_DIR = output_dir,
    DSJOBS_JOB_ID = job_id,
    DSJOBS_STEP_INDEX = as.character(step_index))
  if (!is.null(input_dir))
    env_vars <- c(env_vars, DSJOBS_INPUT_DIR = input_dir)
  if (!is.null(step$config)) {
    for (nm in names(step$config)) {
      val <- step$config[[nm]]
      if (is.null(val) || is.list(val)) next
      upper <- toupper(nm)
      if (upper %in% .BLOCKED_ENV_VARS)
        stop("Config key '", nm, "' is blocked for security.", call. = FALSE)
      val_str <- if (length(val) > 1) paste(val, collapse = ",")
                 else as.character(val)
      new_var <- val_str
      names(new_var) <- paste0("DSJOBS_CFG_", upper)
      env_vars <- c(env_vars, new_var)
    }
  }

  proc <- processx::process$new(
    command = command, args = args,
    stdout = file.path(step_dir, "stdout.log"),
    stderr = file.path(step_dir, "stderr.log"),
    env = env_vars, cleanup = TRUE, cleanup_tree = TRUE)

  # Store handle in registry for reliable exit status checking
  key <- paste0(job_id, "_", step_index)
  .proc_registry[[key]] <- proc

  .store_update_job(db, job_id, worker_pid = proc$get_pid())
  .db_log_event(db, job_id, "artifact_started",
    list(step_index = step_index, runner = runner_name, pid = proc$get_pid()))
}

#' Check if a job's artifact step is still running via processx handle
#' @keywords internal
.proc_is_alive <- function(job_id, step_index) {
  key <- paste0(job_id, "_", step_index)
  proc <- .proc_registry[[key]]
  if (is.null(proc)) return(FALSE)  # No handle = assume dead
  proc$is_alive()
}

#' Get exit status from processx handle, clean up registry
#' @keywords internal
.proc_get_exit <- function(job_id, step_index) {
  key <- paste0(job_id, "_", step_index)
  proc <- .proc_registry[[key]]
  if (is.null(proc)) return(NA_integer_)
  status <- proc$get_exit_status()
  # Clean up handle
  rm(list = key, envir = .proc_registry)
  status
}

#' @keywords internal
.resolve_python_env <- function(runner_config) {
  # If runner specifies an explicit python path, use it
  if (!is.null(runner_config$python)) {
    if (file.exists(runner_config$python))
      return(list(python = runner_config$python))
  }

  # System python fallback
  py <- Sys.which("python3")
  if (!nzchar(py)) py <- Sys.which("python")
  if (!nzchar(py)) py <- "python3"
  list(python = py)
}

#' @keywords internal
.build_runner_args <- function(runner_config, step, step_dir, input_dir) {
  tmpl <- runner_config$args_template
  if (is.null(tmpl)) return(character(0))
  in_dir <- input_dir %||% step_dir
  out_dir <- file.path(step_dir, "output")
  vapply(tmpl, function(a) {
    a <- gsub("\\{input_dir\\}", in_dir, a)
    a <- gsub("\\{output_dir\\}", out_dir, a)
    a <- gsub("\\{step_dir\\}", step_dir, a)
    if (!is.null(step$config))
      for (nm in names(step$config))
        a <- gsub(paste0("\\{", nm, "\\}"), as.character(step$config[[nm]]), a)
    a
  }, character(1))
}
