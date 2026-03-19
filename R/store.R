# Module: Durable Queue Store
# Filesystem-based job persistence under DSJOBS_HOME/queue/.

#' Generate a unique job ID
#'
#' Format: job_YYYYMMDD_HHMMSS_PID_XXXXXXXXXXXX
#'
#' @return Character; the job ID.
#' @keywords internal
.generate_job_id <- function() {
  hex <- paste(sample(c(0:9, letters[1:6]), 12, replace = TRUE), collapse = "")
  paste0("job_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", Sys.getpid(), "_", hex)
}

#' Create a new job in the store
#'
#' @param job_id Character; the job ID.
#' @param spec List; the validated job specification.
#' @return Invisible TRUE.
#' @keywords internal
.store_create_job <- function(job_id, spec) {
  home <- .dsjobs_home()
  job_dir <- file.path(home, "queue", job_id)
  dir.create(job_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(job_dir, "steps"), showWarnings = FALSE)
  dir.create(file.path(job_dir, "result"), showWarnings = FALSE)
  Sys.chmod(job_dir, "0700")

  # Write immutable spec
  spec_path <- file.path(job_dir, "spec.json")
  jsonlite::write_json(spec, spec_path,
                        auto_unbox = TRUE, pretty = TRUE, null = "null")
  Sys.chmod(spec_path, "0600")

  # Write initial state
  state <- list(
    state = "PENDING",
    step_index = 0L,
    total_steps = length(spec$steps),
    submitted_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    started_at = NULL,
    finished_at = NULL,
    error = NULL,
    retries = 0L
  )
  .store_write_state(job_id, state)

  # Initialize audit trail
  .audit_log(job_id, "created", list(total_steps = length(spec$steps)))

  invisible(TRUE)
}

#' Read job state
#'
#' @param job_id Character; the job ID.
#' @return Named list of state, or NULL if not found.
#' @keywords internal
.store_read_state <- function(job_id) {
  home <- .dsjobs_home()
  state_path <- file.path(home, "queue", job_id, "state.json")
  if (!file.exists(state_path)) return(NULL)
  jsonlite::fromJSON(state_path, simplifyVector = FALSE)
}

#' Write job state atomically
#'
#' @param job_id Character; the job ID.
#' @param state Named list of state fields.
#' @return Invisible TRUE.
#' @keywords internal
.store_write_state <- function(job_id, state) {
  home <- .dsjobs_home()
  state_path <- file.path(home, "queue", job_id, "state.json")
  tmp_path <- paste0(state_path, ".tmp")
  jsonlite::write_json(state, tmp_path,
                        auto_unbox = TRUE, pretty = TRUE, null = "null")
  file.rename(tmp_path, state_path)
  Sys.chmod(state_path, "0600")
  invisible(TRUE)
}

#' Update specific fields of job state
#'
#' @param job_id Character; the job ID.
#' @param updates Named list of fields to update.
#' @return The updated state.
#' @keywords internal
.store_update_state <- function(job_id, updates) {
  state <- .store_read_state(job_id)
  if (is.null(state)) {
    stop("Job not found: ", job_id, call. = FALSE)
  }
  for (nm in names(updates)) {
    state[[nm]] <- updates[[nm]]
  }
  .store_write_state(job_id, state)
  state
}

#' Read job spec
#'
#' @param job_id Character; the job ID.
#' @return Named list of spec, or NULL.
#' @keywords internal
.store_read_spec <- function(job_id) {
  home <- .dsjobs_home()
  spec_path <- file.path(home, "queue", job_id, "spec.json")
  if (!file.exists(spec_path)) return(NULL)
  jsonlite::fromJSON(spec_path, simplifyVector = FALSE)
}

#' List all jobs in the store
#'
#' @param states Character vector; filter by state (NULL for all).
#' @return Data.frame with job_id, state, submitted_at, step_index, total_steps.
#' @keywords internal
.store_list_jobs <- function(states = NULL) {
  home <- .dsjobs_home()
  queue_dir <- file.path(home, "queue")
  if (!dir.exists(queue_dir)) {
    return(data.frame(
      job_id = character(0), state = character(0),
      submitted_at = character(0), step_index = integer(0),
      total_steps = integer(0), stringsAsFactors = FALSE
    ))
  }

  job_dirs <- list.dirs(queue_dir, full.names = FALSE, recursive = FALSE)
  if (length(job_dirs) == 0) {
    return(data.frame(
      job_id = character(0), state = character(0),
      submitted_at = character(0), step_index = integer(0),
      total_steps = integer(0), stringsAsFactors = FALSE
    ))
  }

  rows <- lapply(job_dirs, function(jid) {
    st <- .store_read_state(jid)
    if (is.null(st)) return(NULL)
    if (!is.null(states) && !st$state %in% states) return(NULL)
    data.frame(
      job_id = jid,
      state = st$state,
      submitted_at = st$submitted_at %||% "",
      step_index = as.integer(st$step_index %||% 0L),
      total_steps = as.integer(st$total_steps %||% 0L),
      stringsAsFactors = FALSE
    )
  })

  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0) {
    return(data.frame(
      job_id = character(0), state = character(0),
      submitted_at = character(0), step_index = integer(0),
      total_steps = integer(0), stringsAsFactors = FALSE
    ))
  }

  do.call(rbind, rows)
}

#' Get the step directory for a job step
#'
#' @param job_id Character; the job ID.
#' @param step_index Integer; the step index (1-based).
#' @return Character; path to step directory.
#' @keywords internal
.store_step_dir <- function(job_id, step_index) {
  home <- .dsjobs_home()
  step_dir <- file.path(home, "queue", job_id, "steps",
                         sprintf("step_%03d", step_index))
  dir.create(step_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(step_dir, "output"), showWarnings = FALSE)
  step_dir
}
