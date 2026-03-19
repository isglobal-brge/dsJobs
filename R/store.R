# Module: Transactional Store
# CRUD on jobs/steps/events tables. All mutations use transactions.

#' Create a new job in the database
#' @keywords internal
.store_create_job <- function(db, job_id, owner_id, spec, total_steps) {
  spec_json <- as.character(jsonlite::toJSON(spec, auto_unbox = TRUE, null = "null"))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    DBI::dbExecute(db,
      "INSERT INTO jobs (job_id, owner_id, state, step_index, total_steps,
                         resource_class, submitted_at, spec_json)
       VALUES (?, ?, 'PENDING', 0, ?, ?, ?, ?)",
      params = list(job_id, owner_id, total_steps,
                     spec$resource_class %||% "default", now, spec_json))

    # Pre-populate steps table
    for (i in seq_along(spec$steps)) {
      s <- spec$steps[[i]]
      DBI::dbExecute(db,
        "INSERT INTO steps (job_id, step_index, type, plane, runner, state)
         VALUES (?, ?, ?, ?, ?, 'pending')",
        params = list(job_id, i, s$type, s$plane, s$runner %||% NA_character_))
    }

    .db_log_event(db, job_id, "created", list(total_steps = total_steps, owner = owner_id))
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
  invisible(TRUE)
}

#' Read job state
#' @keywords internal
.store_get_job <- function(db, job_id) {
  row <- DBI::dbGetQuery(db,
    "SELECT * FROM jobs WHERE job_id = ?",
    params = list(job_id))
  if (nrow(row) == 0) return(NULL)
  as.list(row[1, ])
}

#' Read job spec
#' @keywords internal
.store_get_spec <- function(db, job_id) {
  row <- DBI::dbGetQuery(db,
    "SELECT spec_json FROM jobs WHERE job_id = ?",
    params = list(job_id))
  if (nrow(row) == 0) return(NULL)
  jsonlite::fromJSON(row$spec_json[1], simplifyVector = FALSE)
}

#' Update job fields atomically
#' @keywords internal
.store_update_job <- function(db, job_id, ...) {
  updates <- list(...)
  if (length(updates) == 0) return(invisible(TRUE))

  set_clauses <- paste0(names(updates), " = ?")
  sql <- paste0("UPDATE jobs SET ", paste(set_clauses, collapse = ", "),
                " WHERE job_id = ?")
  params <- c(unname(updates), list(job_id))
  DBI::dbExecute(db, sql, params = params)
  invisible(TRUE)
}

#' Update step fields atomically
#' @keywords internal
.store_update_step <- function(db, job_id, step_index, ...) {
  updates <- list(...)
  if (length(updates) == 0) return(invisible(TRUE))

  set_clauses <- paste0(names(updates), " = ?")
  sql <- paste0("UPDATE steps SET ", paste(set_clauses, collapse = ", "),
                " WHERE job_id = ? AND step_index = ?")
  params <- c(unname(updates), list(job_id, step_index))
  DBI::dbExecute(db, sql, params = params)
  invisible(TRUE)
}

#' List jobs with optional filters
#' @keywords internal
.store_list_jobs <- function(db, owner_id = NULL, states = NULL) {
  where_parts <- character(0)
  params <- list()

  if (!is.null(owner_id)) {
    where_parts <- c(where_parts, "owner_id = ?")
    params <- c(params, list(owner_id))
  }
  if (!is.null(states)) {
    placeholders <- paste(rep("?", length(states)), collapse = ", ")
    where_parts <- c(where_parts, paste0("state IN (", placeholders, ")"))
    params <- c(params, as.list(states))
  }

  sql <- "SELECT job_id, state, submitted_at, step_index, total_steps FROM jobs"
  if (length(where_parts) > 0) {
    sql <- paste(sql, "WHERE", paste(where_parts, collapse = " AND "))
  }
  sql <- paste(sql, "ORDER BY submitted_at DESC")

  if (length(params) > 0) {
    DBI::dbGetQuery(db, sql, params = params)
  } else {
    DBI::dbGetQuery(db, sql)
  }
}

#' Assert the current user owns the job
#' @keywords internal
.assert_owner <- function(db, job_id) {
  owner <- .get_owner_id()
  row <- DBI::dbGetQuery(db,
    "SELECT owner_id FROM jobs WHERE job_id = ?",
    params = list(job_id))
  if (nrow(row) == 0) {
    stop("Job not found: ", job_id, call. = FALSE)
  }
  if (!identical(row$owner_id[1], owner)) {
    stop("Access denied: job belongs to another user.", call. = FALSE)
  }
  invisible(TRUE)
}
