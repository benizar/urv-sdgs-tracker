# File: tests/testthat/test_translate_helpers.R

test_that("translation_file_path builds the expected filename", {
  skip_if_not(exists("translation_file_path"))
  
  cfg <- list(
    output_dir  = "sandbox/translations",
    service     = "LibreTranslate",
    target_lang = "en"
  )
  
  p <- translation_file_path(cfg, "course_description")
  expect_equal(p, file.path("sandbox/translations", "course_description-en-libretranslate.csv"))
})

test_that("ensure_translation_file behaves by mode", {
  skip_if_not(exists("ensure_translation_file"))
  
  td <- tempfile("tr_test_")
  dir.create(td, recursive = TRUE, showWarnings = FALSE)
  
  cfg_auto <- list(
    mode = "auto",
    output_dir = td,
    service = "libretranslate",
    target_lang = "en"
  )
  
  p_auto <- ensure_translation_file(cfg_auto, "course_description")
  expect_true(file.exists(p_auto))
  
  cfg_rev <- cfg_auto
  cfg_rev$mode <- "reviewer"
  file.remove(p_auto)
  
  expect_error(
    ensure_translation_file(cfg_rev, "course_description"),
    regexp = "Reviewer mode is enabled but translation file does not exist"
  )
  
  file.create(p_auto)
  expect_equal(ensure_translation_file(cfg_rev, "course_description"), p_auto)
})
