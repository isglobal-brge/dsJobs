# Module: SQLite Database
# Connection management, schema creation, and migrations.

#' Open a connection to the dsJobs database
#'
#' Creates the database and schema if they don't exist.
#' Uses WAL mode for concurrent readers.
#'
#' @return A DBI connection object.
#' @keywords internal
.db_connect <- function() {
  home <- .dsjobs_home()
  db_path <- file.path(home, "dsjobs.sqlite")

  first_time <- !file.exists(db_path)

  db <- DBI::dbConnect(RSQLite::SQLite(), db_path)

  # WAL mode: concurrent readers, single writer with automatic retry

  DBI::dbExecute(db, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(db, "PRAGMA busy_timeout=5000")
  DBI::dbExecute(db, "PRAGMA foreign_keys=ON")

  if (first_time) {
    .db_create_schema(db)
  }

  db
}

#' Create the database schema
#' @keywords internal
.db_create_schema <- function(db) {
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS jobs (
      job_id       TEXT PRIMARY KEY,
      owner_id     TEXT NOT NULL,
      state        TEXT NOT NULL DEFAULT 'PENDING',
      step_index   INTEGER NOT NULL DEFAULT 0,
      total_steps  INTEGER NOT NULL,
      resource_class TEXT DEFAULT 'default',
      submitted_at TEXT NOT NULL,
      started_at   TEXT,
      finished_at  TEXT,
      error        TEXT,
      retries      INTEGER NOT NULL DEFAULT 0,
      worker_pid   INTEGER,
      spec_json    TEXT NOT NULL
    )
  ")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS steps (
      job_id       TEXT NOT NULL,
      step_index   INTEGER NOT NULL,
      type         TEXT NOT NULL,
      plane        TEXT NOT NULL,
      runner       TEXT,
      state        TEXT NOT NULL DEFAULT 'pending',
      started_at   TEXT,
      finished_at  TEXT,
      exit_code    INTEGER,
      error        TEXT,
      output_ref   TEXT,
      PRIMARY KEY (job_id, step_index),
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )
  ")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS events (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id       TEXT NOT NULL,
      event        TEXT NOT NULL,
      timestamp    TEXT NOT NULL,
      details_json TEXT,
      FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )
  ")

  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_jobs_owner ON jobs(owner_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id)")
}

#' Close a database connection safely
#' @keywords internal
.db_close <- function(db) {
  tryCatch(DBI::dbDisconnect(db), error = function(e) NULL)
}

#' Record an event in the events table
#' @keywords internal
.db_log_event <- function(db, job_id, event, details = NULL) {
  details_json <- if (!is.null(details)) {
    as.character(jsonlite::toJSON(details, auto_unbox = TRUE))
  } else {
    NA_character_
  }
  DBI::dbExecute(db,
    "INSERT INTO events (job_id, event, timestamp, details_json)
     VALUES (?, ?, ?, ?)",
    params = list(job_id, event,
                   format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
                   details_json))
}
