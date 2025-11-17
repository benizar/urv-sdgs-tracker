# File: tests/testthat/test_assign_faculty_name.R

test_that("assign_faculty_name() recognises known centres and returns NA otherwise", {
  known_url <- "https://guiadocent.urv.cat/guido/public/centres/503/some/path"
  unknown_url <- "https://guiadocent.urv.cat/guido/public/centres/999/some/path"
  
  expect_equal(
    assign_faculty_name(known_url),
    "Facultat de Turisme i Geografia"
  )
  
  expect_true(is.na(assign_faculty_name(unknown_url)))
})
