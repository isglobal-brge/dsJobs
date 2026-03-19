# Module: Job Spec Validation + Security
# Validates job specifications. Fully generic -- no domain concepts.

#' Validate a job specification
#' @keywords internal
.validate_job_spec <- function(spec) {
  if (!is.list(spec)) {
    stop("Job spec must be a list.", call. = FALSE)
  }

  steps <- spec$steps
  if (is.null(steps) || length(steps) == 0) {
    stop("Job spec must contain at least one step.", call. = FALSE)
  }

  settings <- .dsjobs_settings()
  if (length(steps) > settings$max_steps_per_job) {
    stop("Job exceeds maximum steps (", settings$max_steps_per_job, ").", call. = FALSE)
  }

  # Check spec size (serialized)
  spec_size <- nchar(jsonlite::toJSON(spec, auto_unbox = TRUE))
  if (spec_size > settings$max_spec_bytes) {
    stop("Job spec exceeds maximum size (", settings$max_spec_bytes, " bytes).", call. = FALSE)
  }

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

    # Artifact steps must reference valid runner
    if (identical(plane, "artifact")) {
      runner_name <- step$runner
      if (is.null(runner_name)) {
        stop("Artifact step ", i, " must specify a 'runner'.", call. = FALSE)
      }
      .validate_identifier(runner_name, paste0("Step ", i, " runner"))
      runner_config <- .load_runner_config(runner_name)
      if (is.null(runner_config)) {
        stop("Runner '", runner_name, "' not found in allowlist.", call. = FALSE)
      }
      .validate_runner_params(step, runner_config, i)
    }

    # Validate identifiers in publish steps
    if (!is.null(step$dataset_id)) {
      .validate_identifier(step$dataset_id, paste0("Step ", i, " dataset_id"))
    }
    if (!is.null(step$asset_name)) {
      .validate_identifier(step$asset_name, paste0("Step ", i, " asset_name"))
    }
  }

  spec$steps <- steps
  if (is.null(spec$resource_class)) spec$resource_class <- "default"
  spec
}

#' @keywords internal
.infer_step_plane <- function(step_type) {
  session_types <- c("assign_table", "assign_resource", "assign_expr",
                      "aggregate", "emit", "resolve_dataset", "safe_summary",
                      "publish_asset", "publish_dataset")
  if (step_type %in% session_types) return("session")
  "artifact"
}

#' @keywords internal
.load_runner_config <- function(runner_name) {
  if (!grepl("^[a-zA-Z0-9_]+$", runner_name)) return(NULL)

  home <- .dsjobs_home(must_exist = FALSE)
  if (!is.null(home)) {
    custom_path <- file.path(home, "runners", paste0(runner_name, ".yml"))
    if (file.exists(custom_path) && requireNamespace("yaml", quietly = TRUE)) {
      return(yaml::read_yaml(custom_path))
    }
  }

  bundled_path <- system.file("runners", paste0(runner_name, ".yml"),
                               package = "dsJobs")
  if (nzchar(bundled_path) && file.exists(bundled_path)) {
    if (requireNamespace("yaml", quietly = TRUE)) {
      return(yaml::read_yaml(bundled_path))
    }
  }
  NULL
}

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

#' @keywords internal
.list_runners <- function() {
  runners <- character(0)
  bundled_dir <- system.file("runners", package = "dsJobs")
  if (nzchar(bundled_dir) && dir.exists(bundled_dir)) {
    files <- list.files(bundled_dir, pattern = "\\.yml$", full.names = FALSE)
    runners <- c(runners, sub("\\.yml$", "", files))
  }
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
