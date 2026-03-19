# Module: Artifact-Plane Runners
# Launches processx subprocesses. Async -- worker reaps on next poll.

# Blocked env var names (security)
.BLOCKED_ENV_VARS <- c("PATH", "HOME", "USER", "SHELL",
                        "LD_PRELOAD", "LD_LIBRARY_PATH",
                        "DYLD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES",
                        "PYTHONPATH", "PYTHONSTARTUP",
                        "BASH_ENV", "ENV", "CDPATH", "IFS")

#' @keywords internal
.run_artifact_step <- function(db, job_id, step_index, step, step_dir, input_dir) {
  runner_name <- step$runner
  runner_config <- .load_runner_config(runner_name)
  if (is.null(runner_config)) {
    stop("Runner '", runner_name, "' not found.", call. = FALSE)
  }

  # Resolve Python environment
  env_vars <- character(0)
  command <- runner_config$command %||% "python"

  if (identical(command, "python")) {
    python_env <- .resolve_python_env(runner_config)
    command <- python_env$python
  }

  # Build arguments
  args <- .build_runner_args(runner_config, step, step_dir, input_dir)

  # Build safe environment variables
  output_dir <- file.path(step_dir, "output")
  env_vars <- c(
    paste0("DSJOBS_STEP_DIR=", step_dir),
    paste0("DSJOBS_OUTPUT_DIR=", output_dir),
    paste0("DSJOBS_JOB_ID=", job_id),
    paste0("DSJOBS_STEP_INDEX=", step_index)
  )

  if (!is.null(input_dir)) {
    env_vars <- c(env_vars, paste0("DSJOBS_INPUT_DIR=", input_dir))
  }

  # User config -> DSJOBS_CFG_* (with security filtering)
  if (!is.null(step$config)) {
    for (nm in names(step$config)) {
      upper_nm <- toupper(nm)
      if (upper_nm %in% .BLOCKED_ENV_VARS) {
        stop("Config key '", nm, "' is blocked for security.", call. = FALSE)
      }
      val <- as.character(step$config[[nm]])
      env_vars <- c(env_vars, paste0("DSJOBS_CFG_", upper_nm, "=", val))
    }
  }

  # Launch subprocess
  stdout_path <- file.path(step_dir, "stdout.log")
  stderr_path <- file.path(step_dir, "stderr.log")

  proc <- processx::process$new(
    command = command,
    args = args,
    stdout = stdout_path,
    stderr = stderr_path,
    env = env_vars,
    cleanup = TRUE,
    cleanup_tree = TRUE
  )

  # Record PID in database
  .store_update_job(db, job_id, worker_pid = proc$get_pid())

  .db_log_event(db, job_id, "artifact_started", list(
    step_index = step_index, runner = runner_name, pid = proc$get_pid()
  ))
}

#' @keywords internal
.resolve_python_env <- function(runner_config) {
  framework <- runner_config$framework
  if (!is.null(framework) && requireNamespace("dsFlower", quietly = TRUE)) {
    tryCatch({
      env <- dsFlower:::.ensure_python_env(framework)
      return(list(python = env$python))
    }, error = function(e) NULL)
  }
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  if (!nzchar(python)) python <- "python3"
  list(python = python)
}

#' @keywords internal
.build_runner_args <- function(runner_config, step, step_dir, input_dir) {
  args_template <- runner_config$args_template
  if (is.null(args_template)) return(character(0))

  in_dir <- input_dir %||% step_dir
  output_dir <- file.path(step_dir, "output")

  vapply(args_template, function(arg) {
    arg <- gsub("\\{input_dir\\}", in_dir, arg)
    arg <- gsub("\\{output_dir\\}", output_dir, arg)
    arg <- gsub("\\{step_dir\\}", step_dir, arg)
    if (!is.null(step$config)) {
      for (nm in names(step$config)) {
        arg <- gsub(paste0("\\{", nm, "\\}"), as.character(step$config[[nm]]), arg)
      }
    }
    arg
  }, character(1))
}
