# Module: Garbage Collection
# Called by the worker daemon periodically.
# Removes expired jobs from DB and artifacts from disk.
# Also called manually via jobGCDS if needed.

# All GC logic is in worker.R (.worker_gc) since it needs the DB connection.
# This file provides the admin-facing trigger.

#' Trigger garbage collection (admin utility)
#' @keywords internal
.gc_run <- function() {
  db <- .db_connect()
  on.exit(.db_close(db))
  .worker_gc(db)
}
