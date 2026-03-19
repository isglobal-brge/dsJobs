test_that("empty spec is rejected", {
  expect_error(dsJobs:::.validate_job_spec(list()), "at least one step")
})

test_that("spec with too many steps is rejected", {
  withr::local_options(list(dsjobs.max_steps_per_job = 3L))
  spec <- make_test_spec(5)
  expect_error(dsJobs:::.validate_job_spec(spec), "maximum steps")
})

test_that("step without type is rejected", {
  spec <- list(steps = list(list(plane = "session")))
  expect_error(dsJobs:::.validate_job_spec(spec), "missing 'type'")
})

test_that("artifact step without runner is rejected", {
  spec <- list(steps = list(list(type = "run_artifact", plane = "artifact")))
  expect_error(dsJobs:::.validate_job_spec(spec), "must specify 'runner'")
})

test_that("valid session step passes validation", {
  spec <- make_test_spec()
  result <- dsJobs:::.validate_job_spec(spec)
  expect_equal(result$steps[[1]]$plane, "session")
})

test_that("step plane is inferred from type", {
  spec <- list(steps = list(list(type = "aggregate")))
  result <- dsJobs:::.validate_job_spec(spec)
  expect_equal(result$steps[[1]]$plane, "session")
})

test_that("identifier validation blocks path traversal", {
  expect_error(
    dsJobs:::.validate_identifier("../../etc/passwd", "test"),
    "must not contain"
  )
  expect_error(
    dsJobs:::.validate_identifier("foo bar", "test"),
    "invalid characters"
  )
  expect_silent(dsJobs:::.validate_identifier("my_dataset.v1", "test"))
  expect_silent(dsJobs:::.validate_identifier("radiomics-output", "test"))
})

test_that("spec size limit is enforced", {
  withr::local_options(list(dsjobs.max_spec_bytes = 100L))
  big_spec <- list(
    steps = list(list(type = "emit", plane = "session",
                       output_name = "x", value = paste(rep("A", 200), collapse = "")))
  )
  expect_error(dsJobs:::.validate_job_spec(big_spec), "maximum size")
})

test_that("publish step identifiers are validated", {
  spec <- list(steps = list(list(
    type = "publish_asset", plane = "session",
    dataset_id = "../../evil", asset_name = "ok"
  )))
  expect_error(dsJobs:::.validate_job_spec(spec), "must not contain")
})
