# File: tests/testthat/test_common_utils.R

test_that("%||% works as expected", {
  expect_equal(NULL %||% "x", "x")
  expect_equal("y" %||% "x", "y")
})

test_that("rtrim_slash trims only trailing slashes", {
  skip_if_not(exists("rtrim_slash"))
  expect_equal(rtrim_slash("http://x/y/"), "http://x/y")
  expect_equal(rtrim_slash("http://x/y"), "http://x/y")
  expect_equal(rtrim_slash("http://x/y///"), "http://x/y")
})
