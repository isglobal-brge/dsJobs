# Module: Job Spec Validation + Security
# Validates job specifications against allowlisted runners.

#' Validate a job specification
#'
#' Checks that the spec has required fields, all steps reference valid
#' runners, and parameters are within allowed bounds.
#'
#' @param spec Named list; the job specification.
#' @return The validated spec (possibly with defaults filled in).
#' @keywords internal
.validate_job_spec <- function(spec) {
  if (!is.list(spec)) {
    stop("Job spec must be a list.", call. = FALSE)
  }

  steps <- spec$steps
  if (is.null(steps) || length(steps) == 0) {
    stop("Job spec must contain at least one step.", call. = FALSE)
  }

  # Validate each step
  for (i in seq_along(steps)) {
    step <- steps[[i]]
    if (!is.list(step)) {
      stop("Step ", i, " must be a list.", call. = FALSE)
    }
    if (is.null(step$type)) {
      stop("Step ", i, " missing required field 'type'.", call. = FALSE)
    }

    plane <- step$plane %||% .infer_step_plane(step$type)
    if (!plane %in% c("session", "artifact")) {
      stop("Step ", i, " has invalid plane: '", plane, "'.", call. = FALSE)
    }
    steps[[i]]$plane <- plane

    # Artifact steps must reference a valid runner
    if (identical(plane, "artifact")) {
      runner_name <- step$runner
      if (is.null(runner_name)) {
        stop("Artifact step ", i, " must specify a 'runner'.", call. = FALSE)
      }
      runner_config <- .load_runner_config(runner_name)
      if (is.null(runner_config)) {
        stop("Runner '", runner_name, "' not found in allowlist.", call. = FALSE)
      }
      .validate_runner_params(step, runner_config, i)
    }
  }

  spec$steps <- steps

  # Set resource_class default
  if (is.null(spec$resource_class)) {
    spec$resource_class <- "default"
  }

  spec
}

#' Infer the execution plane from step type
#'
#' @param step_type Character; the step type.
#' @return Character; "session" or "artifact".
#' @keywords internal
.infer_step_plane <- function(step_type) {
  session_types <- c(
    "assign_table", "assign_resource", "assign_expr",
    "aggregate", "checkpoint", "restore_checkpoint",
    "emit", "resolve_dataset", "safe_summary"
  )
  if (step_type %in% session_types) return("session")
  "artifact"
}

#' Load a runner configuration from DSJOBS_HOME or inst
#'
#' @param runner_name Character; name of the runner.
#' @return Named list of runner config, or NULL.
#' @keywords internal
.load_runner_config <- function(runner_name) {
  # Sanitize name: only alphanumeric and underscores
  if (!grepl("^[a-zA-Z0-9_]+$", runner_name)) {
    return(NULL)
  }

  # Check DSJOBS_HOME/runners/ first (admin overrides)
  home <- .dsjobs_home(must_exist = FALSE)
  if (!is.null(home)) {
    custom_path <- file.path(home, "runners", paste0(runner_name, ".yml"))
    if (file.exists(custom_path)) {
      if (requireNamespace("yaml", quietly = TRUE)) {
        return(yaml::read_yaml(custom_path))
      }
    }
  }

  # Fall back to bundled runners
  bundled_path <- system.file("runners", paste0(runner_name, ".yml"),
                               package = "dsJobs")
  if (nzchar(bundled_path) && file.exists(bundled_path)) {
    if (requireNamespace("yaml", quietly = TRUE)) {
      return(yaml::read_yaml(bundled_path))
    }
  }

  NULL
}

#' Validate step parameters against runner config
#'
#' @param step Named list; the step.
#' @param runner_config Named list; the runner config.
#' @param step_index Integer; for error messages.
#' @keywords internal
.validate_runner_params <- function(step, runner_config, step_index) {
  allowed <- runner_config$allowed_params
  if (!is.null(allowed) && !is.null(step$config)) {
    disallowed <- setdiff(names(step$config), allowed)
    if (length(disallowed) > 0) {
      stop("Step ", step_index, ": runner '", step$runner,
           "' does not allow parameters: ",
           paste(disallowed, collapse = ", "), call. = FALSE)
    }
  }
  invisible(TRUE)
}

#' Validate runner config structure
#'
#' @param config Named list; runner configuration.
#' @return TRUE invisibly.
#' @keywords internal
.validate_runner_config <- function(config) {
  required <- c("name", "plane", "command")
  missing <- setdiff(required, names(config))
  if (length(missing) > 0) {
    stop("Runner config missing required fields: ",
         paste(missing, collapse = ", "), call. = FALSE)
  }
  if (!config$plane %in% c("session", "artifact")) {
    stop("Runner plane must be 'session' or 'artifact'.", call. = FALSE)
  }
  invisible(TRUE)
}

#' List available runners
#'
#' @return Character vector of runner names.
#' @keywords internal
.list_runners <- function() {
  runners <- character(0)

  # Bundled runners
  bundled_dir <- system.file("runners", package = "dsJobs")
  if (nzchar(bundled_dir) && dir.exists(bundled_dir)) {
    files <- list.files(bundled_dir, pattern = "\\.yml$", full.names = FALSE)
    runners <- c(runners, sub("\\.yml$", "", files))
  }

  # Admin runners in DSJOBS_HOME
  home <- .dsjobs_home(must_exist = FALSE)
  if (!is.null(home)) {
    admin_dir <- file.path(home, "runners")
    if (dir.exists(admin_dir)) {
      files <- list.files(admin_dir, pattern = "\\.yml$", full.names = FALSE)
      runners <- c(runners, sub("\\.yml$", "", files))
    }
  }

  unique(runners)
}
