test_that("per-user quota blocks excess submissions", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home, dsjobs.max_jobs_per_user = 2L))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsJobs:::.store_create_job(db, "job_q1", "user_a", spec, 1L)
  dsJobs:::.store_create_job(db, "job_q2", "user_a", spec, 1L)

  expect_error(dsJobs:::.check_quotas(db, "user_a"), "Per-user quota")
})

test_that("global quota blocks excess submissions", {
  home <- setup_test_home()
  withr::local_options(list(
    dsjobs.home = home,
    dsjobs.max_jobs_per_user = 10L,
    dsjobs.max_jobs_global = 2L
  ))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsJobs:::.store_create_job(db, "job_g1", "user_a", spec, 1L)
  dsJobs:::.store_create_job(db, "job_g2", "user_b", spec, 1L)

  expect_error(dsJobs:::.check_quotas(db, "user_c"), "Global job quota")
})

test_that("completed jobs don't count against quota", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home, dsjobs.max_jobs_per_user = 2L))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsJobs:::.store_create_job(db, "job_done1", "user_a", spec, 1L)
  dsJobs:::.store_update_job(db, "job_done1", state = "FINISHED")
  dsJobs:::.store_create_job(db, "job_done2", "user_a", spec, 1L)
  dsJobs:::.store_update_job(db, "job_done2", state = "FAILED")

  # Should be fine -- both are terminal
  expect_silent(dsJobs:::.check_quotas(db, "user_a"))
})
