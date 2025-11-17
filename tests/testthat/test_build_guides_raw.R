# File: tests/testthat/test_build_guides_raw.R
# Tests for build_guides_raw() and get_guides_raw()

context("build_guides_raw and get_guides_raw")

# Helper to create a minimal import directory with all required CSV files
create_minimal_import_dir <- function() {
  import_dir <- tempfile("guides_import_")
  dir.create(import_dir)
  
  # 1) degree_programs_list.csv (4 columns, ; delimited)
  degree_programs <- data.frame(
    v1 = 1,
    v2 = "https://example.test/faculty/1",
    v3 = "Example degree (2024)",
    v4 = "https://example.test/degree/1/detall",
    stringsAsFactors = FALSE
  )
  readr::write_delim(
    degree_programs,
    file.path(import_dir, "degree_programs_list.csv"),
    delim = ";"
  )
  
  # 2) course_details_list_guido.csv (10 columns)
  cd_guido <- data.frame(
    v1  = 1L,
    v2  = "https://example.test/degree/1/assignatures/gestionar",
    v3  = "C001",
    v4  = "Course from GUIdO",
    v5  = "Presencial",
    v6  = "https://example.test/course/1",
    v7  = "Q1",
    v8  = "OBL",
    v9  = 6,
    v10 = 1,
    stringsAsFactors = FALSE
  )
  readr::write_csv(
    cd_guido,
    file.path(import_dir, "course_details_list_guido.csv")
  )
  
  # 3) course_details_list_docnet.csv (10 columns)
  cd_docnet <- data.frame(
    v1  = 1L,
    v2  = "https://example.test/degree/1/assignatures/gestionar",
    v3  = "Presencial",
    v4  = "C001",
    v5  = "Course from DOCNET",
    v6  = "https://example.test/course/1",
    v7  = "Q1",
    v8  = "OBL",
    v9  = 6,
    v10 = 1,
    stringsAsFactors = FALSE
  )
  readr::write_csv(
    cd_docnet,
    file.path(import_dir, "course_details_list_docnet.csv")
  )
  
  # 4) docnet-iframe.csv (semicolon; needs 'web-scraper-start-url' and 'iframes')
  iframe_path <- file.path(import_dir, "docnet-iframe.csv")
  lines <- c(
    "web-scraper-start-url;iframes",
    "https://example.test/course/1;https://example.test/iframe/1"
  )
  writeLines(lines, iframe_path)
  
  # ---------- Coordinators ----------
  # guido-course-coordinators.csv (4 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Coordinator A",
      v4 = "https://example.test/person/a",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "guido-course-coordinators.csv")
  )
  
  # docnet-course-coordinators.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Coordinator B",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "docnet-course-coordinators.csv")
  )
  
  # ---------- Professors ----------
  # guido-course-professors.csv (4 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Professor A",
      v4 = "https://example.test/person/pA",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "guido-course-professors.csv")
  )
  
  # docnet-course-professors.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Professor B",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "docnet-course-professors.csv")
  )
  
  # ---------- Description ----------
  # guido-course-description.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Description from GUIdO.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "guido-course-description.csv")
  )
  
  # docnet-course-description.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Description from DOCNET.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "docnet-course-description.csv")
  )
  
  # ---------- Contents ----------
  # guido-course-contents.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Contents from GUIdO.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "guido-course-contents.csv")
  )
  
  # docnet-course-contents.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Contents from DOCNET.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "docnet-course-contents.csv")
  )
  
  # ---------- Learning results ----------
  # guido-course-learning-results.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Learning results GUIdO.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "guido-course-learning-results.csv")
  )
  
  # docnet-course-learning-results.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Learning results DOCNET.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "docnet-course-learning-results.csv")
  )
  
  # docnet-course-competences.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Competences DOCNET.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "docnet-course-competences.csv")
  )
  
  # ---------- Bibliography ----------
  # guido-course-bibliography.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Bibliography GUIdO.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "guido-course-bibliography.csv")
  )
  
  # docnet-course-bibliography.csv (3 columns)
  readr::write_csv(
    data.frame(
      v1 = 1L,
      v2 = "https://example.test/course/1",
      v3 = "Bibliography DOCNET.",
      stringsAsFactors = FALSE
    ),
    file.path(import_dir, "docnet-course-bibliography.csv")
  )
  
  import_dir
}

test_that("build_guides_raw() returns expected list structure", {
  import_dir <- create_minimal_import_dir()
  on.exit(unlink(import_dir, recursive = TRUE), add = TRUE)
  
  raw <- build_guides_raw(import_dir)
  
  expect_type(raw, "list")
  expect_true(all(c(
    "degree_programs",
    "course_details",
    "course_coordinators",
    "course_professors",
    "course_description",
    "course_contents",
    "course_competences_learning_results",
    "course_bibliography"
  ) %in% names(raw)))
  
  # Some quick sanity checks on data frames
  expect_s3_class(raw$degree_programs, "data.frame")
  expect_s3_class(raw$course_details, "data.frame")
  expect_s3_class(raw$course_description, "data.frame")
})

test_that("get_guides_raw() works with a minimal YAML config", {
  import_dir <- create_minimal_import_dir()
  on.exit(unlink(import_dir, recursive = TRUE), add = TRUE)
  
  tmp_root <- tempfile("sdgtest_cfg_")
  dir.create(tmp_root)
  
  # We store the import_dir under an "archive" base dir to reuse resolve_import_dir()
  archive_base_dir <- file.path(tmp_root, "archive")
  dir.create(archive_base_dir, recursive = TRUE)
  run_id <- "test_run"
  final_import_dir <- file.path(archive_base_dir, paste0("guides_scraping_", run_id))
  dir.create(final_import_dir, recursive = TRUE)
  
  # Move everything we created to the directory that resolve_import_dir() expects
  file.copy(
    from      = list.files(import_dir, full.names = TRUE),
    to        = final_import_dir,
    recursive = TRUE
  )
  
  # Build a temporary scraping config YAML pointing to this archive.
  cfg_path <- file.path(tmp_root, "scraping-test.yml")
  yaml::write_yaml(
    list(
      mode             = "existing",
      root_url         = "https://example.test",
      base_dir         = "data",
      sandbox_base_dir = "sandbox/data",
      archive_base_dir = archive_base_dir,
      input_location   = "archive",
      run_id           = run_id
    ),
    cfg_path
  )
  
  cfg <- load_scraping_config(cfg_path)
  
  raw <- get_guides_raw(cfg)
  
  expect_type(raw, "list")
  expect_true("course_details" %in% names(raw))
  expect_gt(nrow(raw$course_details), 0)
})
