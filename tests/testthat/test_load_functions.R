# File: tests/testthat/test_load_functions.R

test_that("resolve_load_dir builds the expected path", {
  skip_if_not(exists("resolve_load_dir"))
  
  cfg1 <- list(root = "data", dir = "urv_teaching_guides", subdir = "scraped-20251127")
  expect_equal(resolve_load_dir(cfg1), file.path("data", "urv_teaching_guides", "scraped-20251127"))
  
  cfg2 <- list(root = "data", dir = "urv_teaching_guides", subdir = "")
  expect_equal(resolve_load_dir(cfg2), file.path("data", "urv_teaching_guides"))
})

test_that("build_guides_index_from_loaded returns expected columns (toy data)", {
  skip_if_not(exists("build_guides_index_from_loaded"))
  
  centres_list <- tibble::tibble(
    centre_code = c("001"),
    centre_name = c("Centre A")
  )
  
  programmes_list <- tibble::tibble(
    centre_programme_code = c("001_100"),
    programme_name = c("Prog A")
  )
  
  course_details_list <- tibble::tibble(
    source_system = c("GUIDO", "DOCnet"),
    academic_year = c("2024-2025", "2024-2025"),
    centre_code = c("001", "001"),
    centre_programme_code = c("001_100", "001_100"),
    centre_programme_course_code = c("001_100_10", "001_100_20"),
    course_url = c("http://guido/10", "http://guido/20"),
    course_name = c("Z course", "A course"),
    course_period = c("Q1", "Q2"),
    course_type = c("OB", "OB"),
    course_credits = c(6, 6)
  )
  
  guido_docnet_course_code_map <- tibble::tibble(
    guido_centre_programme_course_code  = c("001_100_20"),
    docnet_centre_programme_course_code = c("001_100_200"),
    docnet_course_url = c("http://docnet/200"),
    docnet_modality = c("v")
  )
  
  guido_course_info <- tibble::tibble(
    guido_centre_programme_course_code = c("001_100_10"),
    course_description = c("desc guido"),
    course_contents = c("cont guido"),
    course_competences_and_results = c("comp guido"),
    course_references = c("ref guido")
  )
  
  docnet_course_info <- tibble::tibble(
    docnet_centre_programme_course_code = c("001_100_200"),
    course_description = c("desc docnet"),
    course_contents = c("cont docnet"),
    course_competences_and_results = c("comp docnet"),
    course_references = c("ref docnet")
  )
  
  out <- build_guides_index_from_loaded(
    centres_list,
    programmes_list,
    course_details_list,
    guido_docnet_course_code_map,
    docnet_course_info,
    guido_course_info
  )
  
  expect_true(all(c("centre_name", "programme_name", "course_name") %in% names(out)))
  expect_true(all(c("course_description", "course_contents", "course_competences_and_results", "course_references") %in% names(out)))
  expect_equal(nrow(out), 2)
})
