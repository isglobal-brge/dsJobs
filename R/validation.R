# Module: Job Spec Validation
# Fully generic. No domain concepts.

#' @keywords internal
.validate_job_spec <- function(spec) {
  if (!is.list(spec)) stop("Job spec must be a list.", call. = FALSE)
  steps <- spec$steps
  if (is.null(steps) || length(steps) == 0)
    stop("Job spec must contain at least one step.", call. = FALSE)

  settings <- .dsjobs_settings()
  if (length(steps) > settings$max_steps_per_job)
    stop("Job exceeds maximum steps (", settings$max_steps_per_job, ").", call. = FALSE)

  spec_size <- nchar(jsonlite::toJSON(spec, auto_unbox = TRUE))
  if (spec_size > settings$max_spec_bytes)
    stop("Job spec exceeds maximum size.", call. = FALSE)

  for (i in seq_along(steps)) {
    step <- steps[[i]]
    if (!is.list(step)) stop("Step ", i, " must be a list.", call. = FALSE)
    if (is.null(step$type)) stop("Step ", i, " missing 'type'.", call. = FALSE)

    plane <- step$plane %||% .infer_step_plane(step$type)
    if (!plane %in% c("session", "artifact"))
      stop("Step ", i, " invalid plane: '", plane, "'.", call. = FALSE)
    steps[[i]]$plane <- plane

    if (identical(plane, "artifact")) {
      if (is.null(step$runner))
        stop("Artifact step ", i, " must specify 'runner'.", call. = FALSE)
      .validate_identifier(step$runner, paste0("Step ", i, " runner"))
      cfg <- .load_runner_config(step$runner)
      if (is.null(cfg))
        stop("Runner '", step$runner, "' not in allowlist.", call. = FALSE)
      .validate_runner_params(step, cfg, i)
    }
    if (!is.null(step$dataset_id))
      .validate_identifier(step$dataset_id, paste0("Step ", i, " dataset_id"))
    if (!is.null(step$asset_name))
      .validate_identifier(step$asset_name, paste0("Step ", i, " asset_name"))
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
  if (step_type %in% session_types) "session" else "artifact"
}

#' @keywords internal
.load_runner_config <- function(runner_name) {
  if (!grepl("^[a-zA-Z0-9_]+$", runner_name)) return(NULL)
  home <- .dsjobs_home(must_exist = FALSE)
  if (!is.null(home)) {
    p <- file.path(home, "runners", paste0(runner_name, ".yml"))
    if (file.exists(p) && requireNamespace("yaml", quietly = TRUE))
      return(yaml::read_yaml(p))
  }
  bp <- system.file("runners", paste0(runner_name, ".yml"), package = "dsJobs")
  if (nzchar(bp) && file.exists(bp) && requireNamespace("yaml", quietly = TRUE))
    return(yaml::read_yaml(bp))
  NULL
}

#' @keywords internal
.validate_runner_params <- function(step, runner_config, step_index) {
  allowed <- runner_config$allowed_params
  if (!is.null(allowed) && !is.null(step$config)) {
    bad <- setdiff(names(step$config), allowed)
    if (length(bad) > 0)
      stop("Step ", step_index, ": runner '", step$runner,
           "' does not allow: ", paste(bad, collapse = ", "), call. = FALSE)
  }
}

#' @keywords internal
.list_runners <- function() {
  runners <- character(0)
  bd <- system.file("runners", package = "dsJobs")
  if (nzchar(bd) && dir.exists(bd))
    runners <- sub("\\.yml$", "", list.files(bd, "\\.yml$"))
  home <- .dsjobs_home(must_exist = FALSE)
  if (!is.null(home)) {
    ad <- file.path(home, "runners")
    if (dir.exists(ad))
      runners <- c(runners, sub("\\.yml$", "", list.files(ad, "\\.yml$")))
  }
  unique(runners)
}
