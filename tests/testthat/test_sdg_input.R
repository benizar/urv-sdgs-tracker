# File: tests/testthat/test_sdg_input.R

test_that("build_sdg_input creates one row per (document_number, section)", {
  skip_if_not(exists("build_sdg_input"))
  
  guides_translated <- tibble::tibble(
    document_number = c("d1", "d2"),
    course_description_en = c("text desc 1", "text desc 2"),
    course_contents_en = c("text cont 1", ""),
    course_competences_and_results_en = c("", "text comp 2"),
    course_references_en = c(NA_character_, "text ref 2")
  )
  
  sdg_cfg <- list(
    combine_groups = list(
      list(
        id = "course_info",
        prefix = "Course information: ",
        columns = c("course_description_en", "course_contents_en")
      ),
      list(
        id = "competences",
        prefix = "Competences and learning outcomes: ",
        columns = c("course_competences_and_results_en")
      ),
      list(
        id = "references",
        prefix = "References: ",
        columns = c("course_references_en")
      )
    )
  )
  
  out <- build_sdg_input(guides_translated, sdg_cfg)
  
  expect_true(all(c("document_number", "section", "text") %in% names(out)))
  expect_true(all(out$section %in% c("course_info", "competences", "references")))
  expect_true(any(grepl("^Course information: ", out$text)))
})
