# File: tests/testthat/test_common_utils.R

test_that("%||% returns default for NULL and original value otherwise", {
  # NULL case: returns default
  expect_equal(NULL %||% "default", "default")

  # Non-NULL case: returns original
  expect_equal("value" %||% "default", "value")

  # Empty vector but not NULL: should not trigger default
  expect_equal(character(0) %||% "default", character(0))
})

