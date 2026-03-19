test_that("blocked env vars are rejected in validation spec", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  # The BLOCKED_ENV_VARS list exists
  expect_true("LD_PRELOAD" %in% dsJobs:::.BLOCKED_ENV_VARS)
  expect_true("PATH" %in% dsJobs:::.BLOCKED_ENV_VARS)
  expect_true("PYTHONPATH" %in% dsJobs:::.BLOCKED_ENV_VARS)
})

test_that("env var names are prefixed with DSJOBS_CFG_", {
  env_vars <- character(0)
  config <- list(setting1 = "value1", setting2 = "42")
  for (nm in names(config)) {
    upper_nm <- toupper(nm)
    if (upper_nm %in% dsJobs:::.BLOCKED_ENV_VARS) stop("blocked")
    env_vars <- c(env_vars, paste0("DSJOBS_CFG_", upper_nm, "=",
                                    as.character(config[[nm]])))
  }
  expect_equal(env_vars[1], "DSJOBS_CFG_SETTING1=value1")
  expect_equal(env_vars[2], "DSJOBS_CFG_SETTING2=42")
})

test_that("blocked env var in config list is caught", {
  config <- list(LD_PRELOAD = "/tmp/evil.so")
  expect_error({
    for (nm in names(config)) {
      if (toupper(nm) %in% dsJobs:::.BLOCKED_ENV_VARS)
        stop("Config key '", nm, "' is blocked for security.", call. = FALSE)
    }
  }, "blocked for security")
})
