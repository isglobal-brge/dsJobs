# Module: Artifact-Plane Runners
# Runs heavy processes (radiomics, preprocessing, staging) as processx
# subprocesses with filesystem-based I/O.

#' Run an artifact-plane step
#'
#' Launches a processx subprocess based on the runner configuration.
#' The process runs independently; reaping happens on next scheduler poll.
#'
#' @param job_id Character; the job ID.
#' @param step_index Integer; the step index.
#' @param step Named list; the step spec.
#' @param step_dir Character; path to step directory.
#' @return Invisible NULL.
#' @keywords internal
.run_artifact_step <- function(job_id, step_index, step, step_dir) {
  runner_name <- step$runner
  runner_config <- .load_runner_config(runner_name)
  if (is.null(runner_config)) {
    stop("Runner '", runner_name, "' not found.", call. = FALSE)
  }

  # Resolve Python environment if needed
  env_vars <- character(0)
  command <- runner_config$command %||% "python"

  if (identical(command, "python")) {
    python_env <- .resolve_python_env(runner_config)
    command <- python_env$python
    env_vars <- c(env_vars, paste0("PYTHONPATH=", python_env$pythonpath %||% ""))
  }

  # Build command arguments from template
  args <- .build_runner_args(runner_config, step, step_dir)

  # Set up I/O paths
  stdout_path <- file.path(step_dir, "stdout.log")
  stderr_path <- file.path(step_dir, "stderr.log")
  output_dir <- file.path(step_dir, "output")

  # Environment variables for the subprocess
  env_vars <- c(
    env_vars,
    paste0("DSJOBS_STEP_DIR=", step_dir),
    paste0("DSJOBS_OUTPUT_DIR=", output_dir),
    paste0("DSJOBS_JOB_ID=", job_id),
    paste0("DSJOBS_STEP_INDEX=", step_index)
  )

  # Add step config as env vars
  if (!is.null(step$config)) {
    for (nm in names(step$config)) {
      val <- as.character(step$config[[nm]])
      env_vars <- c(env_vars, paste0("DSJOBS_", toupper(nm), "=", val))
    }
  }

  # Get timeout
  timeout_secs <- as.integer(runner_config$timeout_secs %||%
                              .dsjobs_settings()$default_timeout_secs)

  # Launch subprocess
  proc <- processx::process$new(
    command = command,
    args = args,
    stdout = stdout_path,
    stderr = stderr_path,
    env = env_vars,
    cleanup = TRUE,
    cleanup_tree = TRUE
  )

  # Track the worker
  worker_key <- paste0("worker_", job_id)
  assign(worker_key, proc, envir = .dsjobs_env)

  .audit_log(job_id, "artifact_started", list(
    step_index = step_index,
    runner = runner_name,
    pid = proc$get_pid(),
    timeout_secs = timeout_secs
  ))

  invisible(NULL)
}

#' Resolve Python environment for an artifact runner
#'
#' Uses dsFlower's .ensure_python_env if available, otherwise falls back
#' to system Python.
#'
#' @param runner_config Named list; runner configuration.
#' @return Named list with python, pythonpath.
#' @keywords internal
.resolve_python_env <- function(runner_config) {
  framework <- runner_config$framework %||% NULL

  if (!is.null(framework) && requireNamespace("dsFlower", quietly = TRUE)) {
    tryCatch({
      env <- dsFlower:::.ensure_python_env(framework)
      return(list(
        python = env$python,
        pythonpath = dirname(env$python)
      ))
    }, error = function(e) NULL)
  }

  # Fallback: system Python
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  if (!nzchar(python)) python <- "python3"

  list(python = python, pythonpath = "")
}

#' Build command-line arguments from runner template
#'
#' @param runner_config Named list; runner configuration.
#' @param step Named list; the step spec.
#' @param step_dir Character; path to step directory.
#' @return Character vector of arguments.
#' @keywords internal
.build_runner_args <- function(runner_config, step, step_dir) {
  args_template <- runner_config$args_template
  if (is.null(args_template)) return(character(0))

  input_dir <- step$input_dir %||% step_dir
  output_dir <- file.path(step_dir, "output")

  # Resolve template variables
  args <- vapply(args_template, function(arg) {
    arg <- gsub("\\{input_dir\\}", input_dir, arg)
    arg <- gsub("\\{output_dir\\}", output_dir, arg)
    arg <- gsub("\\{step_dir\\}", step_dir, arg)

    # Resolve config variables
    if (!is.null(step$config)) {
      for (nm in names(step$config)) {
        arg <- gsub(paste0("\\{", nm, "\\}"),
                     as.character(step$config[[nm]]), arg)
      }
    }
    arg
  }, character(1))

  args
}
