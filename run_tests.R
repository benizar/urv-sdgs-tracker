# File: run_tests.R
# Run all testthat tests for the project.

if (!requireNamespace("testthat", quietly = TRUE)) {
  stop("Package 'testthat' is required. Please install it before running tests.")
}

library(testthat)

test_dir("tests/testthat/")
