# File: tests/testthat/test_resolve_import_dir.R

test_that("resolve_import_dir builds correct path for archive and sandbox", {
  # Create temporary root dirs for sandbox and archive
  sandbox_root <- withr::local_tempdir()
  archive_root <- withr::local_tempdir()
  run_id       <- "20251115"

  # Create minimal directories so resolve_import_dir does not fail.
  sandbox_dir <- file.path(sandbox_root, paste0("guides_scraping_", run_id))
  archive_dir <- file.path(archive_root, paste0("guides_scraping_", run_id))
  dir.create(sandbox_dir, recursive = TRUE)
  dir.create(archive_dir, recursive = TRUE)

  # Fake config list
  cfg <- list(
    input_location   = "archive",
    run_id           = run_id,
    sandbox_base_dir = sandbox_root,
    archive_base_dir = archive_root
  )

  # Archive case
  path_archive <- resolve_import_dir(cfg)
  expect_equal(path_archive, archive_dir)

  # Sandbox case
  cfg$input_location <- "sandbox"
  path_sandbox <- resolve_import_dir(cfg)
  expect_equal(path_sandbox, sandbox_dir)

  # Unknown location should error
  cfg$input_location <- "invalid_location"
  expect_error(
    resolve_import_dir(cfg),
    "Unknown input_location"
  )
})

