# Module: SQLite Database
# Source of truth. WAL mode. Expanded schema with outputs, checkpoints.

#' @keywords internal
.db_connect <- function() {
  home <- .dsjobs_home()
  db_path <- file.path(home, "dsjobs.sqlite")
  first_time <- !file.exists(db_path)
  db <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  DBI::dbExecute(db, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(db, "PRAGMA busy_timeout=5000")
  DBI::dbExecute(db, "PRAGMA foreign_keys=ON")
  if (first_time) .db_create_schema(db)
  db
}

#' @keywords internal
.db_create_schema <- function(db) {
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS jobs (
      job_id          TEXT PRIMARY KEY,
      owner_id        TEXT NOT NULL,
      state           TEXT NOT NULL DEFAULT 'PENDING',
      step_index      INTEGER NOT NULL DEFAULT 0,
      total_steps     INTEGER NOT NULL,
      resource_class  TEXT DEFAULT 'default',
      priority        INTEGER DEFAULT 0,
      submitted_at    TEXT NOT NULL,
      accepted_at     TEXT,
      started_at      TEXT,
      finished_at     TEXT,
      error_class     TEXT,
      error_message   TEXT,
      retry_count     INTEGER NOT NULL DEFAULT 0,
      cancel_requested INTEGER NOT NULL DEFAULT 0,
      worker_pid      INTEGER,
      label           TEXT,
      tags            TEXT,
      visibility      TEXT NOT NULL DEFAULT 'private',
      spec_json       TEXT NOT NULL,
      spec_hash       TEXT
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS steps (
      job_id       TEXT NOT NULL,
      step_index   INTEGER NOT NULL,
      type         TEXT NOT NULL,
      plane        TEXT NOT NULL,
      runner       TEXT,
      state        TEXT NOT NULL DEFAULT 'pending',
      input_refs   TEXT,
      output_ref   TEXT,
      started_at   TEXT,
      finished_at  TEXT,
      exit_code    INTEGER,
      error_class  TEXT,
      error_message TEXT,
      PRIMARY KEY (job_id, step_index),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS outputs (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id        TEXT NOT NULL,
      step_index    INTEGER,
      name          TEXT NOT NULL,
      kind          TEXT NOT NULL,
      path_or_ref   TEXT,
      size_bytes    INTEGER,
      safe_for_client INTEGER NOT NULL DEFAULT 0,
      created_at    TEXT NOT NULL,
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS events (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id        TEXT NOT NULL,
      event         TEXT NOT NULL,
      timestamp     TEXT NOT NULL,
      details_json  TEXT,
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )")

  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_jobs_owner ON jobs(owner_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_outputs_job ON outputs(job_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id)")
}

#' @keywords internal
.db_close <- function(db) {
  tryCatch(DBI::dbDisconnect(db), error = function(e) NULL)
}

#' @keywords internal
.db_log_event <- function(db, job_id, event, details = NULL) {
  details_json <- if (!is.null(details))
    as.character(jsonlite::toJSON(details, auto_unbox = TRUE))
  else NA_character_
  DBI::dbExecute(db,
    "INSERT INTO events (job_id, event, timestamp, details_json)
     VALUES (?, ?, ?, ?)",
    params = list(job_id, event,
      format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"), details_json))
}

#' Register an output in the outputs table
#' @keywords internal
.db_register_output <- function(db, job_id, step_index, name, kind,
                                 path_or_ref, size_bytes = NA_integer_,
                                 safe_for_client = FALSE) {
  DBI::dbExecute(db,
    "INSERT INTO outputs (job_id, step_index, name, kind, path_or_ref,
                          size_bytes, safe_for_client, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    params = list(job_id, step_index, name, kind, path_or_ref,
      as.integer(size_bytes), as.integer(safe_for_client),
      format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
}
