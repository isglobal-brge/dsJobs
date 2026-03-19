# Test helpers for dsJobs

#' Create a temporary DSJOBS_HOME for testing
#' @return Character; path to temp home
setup_test_home <- function() {
  home <- file.path(tempdir(), paste0("dsjobs_test_", Sys.getpid()))
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(home, "runners"), showWarnings = FALSE)
  dir.create(file.path(home, "artifacts"), showWarnings = FALSE)
  dir.create(file.path(home, "publish"), showWarnings = FALSE)
  dir.create(file.path(home, "locks"), showWarnings = FALSE)
  home
}

#' Clean up a test home
cleanup_test_home <- function(home) {
  unlink(home, recursive = TRUE)
}

#' Create a minimal valid job spec
make_test_spec <- function(n_steps = 1) {
  steps <- lapply(seq_len(n_steps), function(i) {
    list(type = "emit", plane = "session",
         output_name = paste0("out_", i), value = i)
  })
  list(steps = steps, resource_class = "default")
}
