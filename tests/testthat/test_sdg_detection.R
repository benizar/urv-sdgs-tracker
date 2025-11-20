# File: tests/testthat/test_sdg_detection.R

test_that("build_sdg_input_text() builds one row per document and section", {
  # Minimal guides_translated with *_en columns
  guides_translated <- tibble::tibble(
    document_number                    = c(101L, 102L),
    course_name_en                     = c("Sustainable cities", "Renewable energy"),
    description_en                     = c("Course about SDG11", "Course about SDG7"),
    contents_en                        = c("Urban planning; transport", "Solar and wind power"),
    competences_learning_results_en    = c("Students understand cities", "Students understand energy"),
    references_en                      = c("UN 2030 Agenda", "IPCC reports")
  )
  
  # Config mimicking pipeline.yml combine_groups
  sdg_cfg <- list(
    combine_groups = list(
      list(
        id      = "course_info",
        prefix  = "Course information: ",
        columns = c(
          "course_name_en",
          "description_en",
          "contents_en"
        )
      ),
      list(
        id      = "competences",
        prefix  = "Competences and learning outcomes: ",
        columns = c("competences_learning_results_en")
      ),
      list(
        id      = "references",
        prefix  = "References: ",
        columns = c("references_en")
      )
    )
  )
  
  sdg_input <- build_sdg_input_text(guides_translated, sdg_cfg)
  
  # We expect 2 documents × 3 sections = 6 rows
  expect_equal(nrow(sdg_input), 6L)
  
  # document_number must match original, repeated per section
  expect_true(all(sdg_input$document_number %in% c(101L, 102L)))
  
  # Sections must be exactly the configured ids
  expect_setequal(
    unique(sdg_input$section),
    c("course_info", "competences", "references")
  )
  
  # Text column must be non-empty for all rows
  expect_false(any(is.na(sdg_input$text) | sdg_input$text == ""))
  
  # Quick sanity check: prefixes are actually present
  expect_true(any(grepl("^Course information:", sdg_input$text)))
  expect_true(any(grepl("^Competences and learning outcomes:", sdg_input$text)))
  expect_true(any(grepl("^References:", sdg_input$text)))
})


test_that("run_text2sdg_detection() maps document index to document_number and section", {
  # Fake sdg_input as produced by build_sdg_input_text()
  sdg_input <- tibble::tibble(
    document_number = c(101L, 102L, 103L),
    section         = c("course_info", "competences", "references"),
    text            = c(
      "Text for course 101",
      "Text for course 102",
      "Text for course 103"
    )
  )
  
  # Minimal SDG config
  sdg_cfg <- list(
    method  = "systems",
    systems = c("Aurora", "Elsevier"),
    sdgs    = 1:17,
    verbose = FALSE
  )
  
  # Fake hits as if returned by text2sdg::detect_sdg_systems()
  fake_hits <- tibble::tibble(
    document    = c(1L, 2L, 2L, 3L),
    system      = c("Aurora", "Aurora", "Elsevier", "Aurora"),
    sdg         = c("SDG 11", "SDG 7", "SDG 13", "SDG 4"),
    probability = c(0.9, 0.8, 0.7, 0.6)
  )
  
  # Temporarily mock text2sdg::detect_sdg_systems inside its namespace
  testthat::local_mocked_bindings(
    detect_sdg_systems = function(x, systems, sdgs, output, verbose) {
      fake_hits
    },
    .package = "text2sdg"
  )
  
  hits_out <- run_text2sdg_detection(sdg_input, sdg_cfg)
  
  # We should keep the same number of rows
  expect_equal(nrow(hits_out), nrow(fake_hits))
  
  # document_number and section must map from sdg_input$document
  expect_equal(
    hits_out$document_number,
    c(101L, 102L, 102L, 103L)
  )
  
  expect_equal(
    hits_out$section,
    c("course_info", "competences", "competences", "references")
  )
  
  # Original columns from fake_hits should still be present
  expect_true(all(c("system", "sdg", "probability") %in% names(hits_out)))
})
