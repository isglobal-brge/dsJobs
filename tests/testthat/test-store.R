test_that("job creation persists to database", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec(3)
  dsJobs:::.store_create_job(db, "job_test_create", "user_a", spec, 3L)

  # Verify job row
  job <- dsJobs:::.store_get_job(db, "job_test_create")
  expect_equal(job$job_id, "job_test_create")
  expect_equal(job$owner_id, "user_a")
  expect_equal(job$state, "PENDING")
  expect_equal(as.integer(job$total_steps), 3L)

  # Verify steps rows
  steps <- DBI::dbGetQuery(db, "SELECT * FROM steps WHERE job_id = 'job_test_create'")
  expect_equal(nrow(steps), 3L)
  expect_true(all(steps$state == "pending"))
})

test_that("job update is atomic", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsJobs:::.store_create_job(db, "job_atomic", "user_a", spec, 1L)

  dsJobs:::.store_update_job(db, "job_atomic",
    state = "RUNNING", step_index = 1L)

  job <- dsJobs:::.store_get_job(db, "job_atomic")
  expect_equal(job$state, "RUNNING")
  expect_equal(as.integer(job$step_index), 1L)
})

test_that("listing jobs filters by owner", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsJobs:::.store_create_job(db, "job_owner_a", "user_a", spec, 1L)
  dsJobs:::.store_create_job(db, "job_owner_b", "user_b", spec, 1L)

  jobs_a <- dsJobs:::.store_list_jobs(db, owner_id = "user_a")
  expect_equal(nrow(jobs_a), 1L)
  expect_equal(jobs_a$job_id, "job_owner_a")

  jobs_b <- dsJobs:::.store_list_jobs(db, owner_id = "user_b")
  expect_equal(nrow(jobs_b), 1L)
  expect_equal(jobs_b$job_id, "job_owner_b")

  # All jobs
  jobs_all <- dsJobs:::.store_list_jobs(db)
  expect_equal(nrow(jobs_all), 2L)
})

test_that("listing jobs filters by state", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsJobs:::.store_create_job(db, "job_pending", "user_a", spec, 1L)
  dsJobs:::.store_create_job(db, "job_running", "user_a", spec, 1L)
  dsJobs:::.store_update_job(db, "job_running", state = "RUNNING")

  pending <- dsJobs:::.store_list_jobs(db, states = "PENDING")
  expect_equal(nrow(pending), 1L)
  expect_equal(pending$job_id, "job_pending")

  running <- dsJobs:::.store_list_jobs(db, states = "RUNNING")
  expect_equal(nrow(running), 1L)
})

test_that("ownership assertion blocks unauthorized access", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec()
  dsJobs:::.store_create_job(db, "job_owned", "real_owner", spec, 1L)

  # Mock current user as someone else
  withr::local_envvar(list(USER = "intruder", ROCK_USER = "", OPAL_USER = ""))
  expect_error(dsJobs:::.assert_owner(db, "job_owned"), "Access denied")
})

test_that("spec retrieval returns parsed JSON", {
  home <- setup_test_home()
  withr::local_options(list(dsjobs.home = home))
  on.exit(cleanup_test_home(home))

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)

  spec <- make_test_spec(2)
  dsJobs:::.store_create_job(db, "job_spec", "user_a", spec, 2L)

  retrieved <- dsJobs:::.store_get_spec(db, "job_spec")
  expect_true(is.list(retrieved))
  expect_equal(length(retrieved$steps), 2L)
  expect_equal(retrieved$steps[[1]]$type, "emit")
})
