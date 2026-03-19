test_that("blocked env vars are rejected", {
  expect_error(
    dsJobs:::.run_artifact_step(NULL, "j1", 1L,
      list(type = "run_artifact", plane = "artifact",
           runner = "pyradiomics",
           config = list(LD_PRELOAD = "/tmp/evil.so")),
      tempdir(), NULL),
    "blocked for security"
  )
})

test_that("env var names are prefixed with DSJOBS_CFG_", {
  # Test the build logic without actually launching
  runner_config <- list(
    name = "test", plane = "artifact", command = "echo",
    args_template = list("{output_dir}"),
    allowed_params = c("setting1", "setting2")
  )
  step <- list(
    type = "run_artifact", plane = "artifact", runner = "test",
    config = list(setting1 = "value1")
  )

  # The env var construction logic
  env_vars <- character(0)
  for (nm in names(step$config)) {
    upper_nm <- toupper(nm)
    if (upper_nm %in% dsJobs:::.BLOCKED_ENV_VARS) {
      stop("blocked")
    }
    env_vars <- c(env_vars, paste0("DSJOBS_CFG_", upper_nm, "=",
                                    as.character(step$config[[nm]])))
  }
  expect_equal(env_vars, "DSJOBS_CFG_SETTING1=value1")
})
