# File: tests/testthat/test_build_guides_index.R

test_that("build_guides_index_from_raw() builds a per-course table with expected structure", {
  # Minimal synthetic raw data mimicking the real structure --------------------
  degree_programs <- tibble::tibble(
    web_scraper_order  = 1L,
    faculty_school_url = "https://guiadocent.urv.cat/guido/public/centres/503/",
    degree_name        = "Grau en Turisme (2020)",
    degree_url         = "https://guiadocent.urv.cat/guido/public/centres/503/studies/1234/detall"
  )
  
  course_details <- tibble::tibble(
    web_scraper_order      = 1L,
    degree_url             = "https://guiadocent.urv.cat/guido/public/centres/503/studies/1234/assignatures/gestionar",
    course_url             = "https://guiadocent.urv.cat/guido/public/assignatura/ABC123",
    course_code            = "ABC123",
    course_name            = "Introducció al turisme",
    course_delivery_mode   = "Presencial",
    course_period          = "Q1",
    course_type            = "Obligatòria",
    credits                = 6,
    year                   = 1
  )
  
  course_coordinators <- tibble::tibble(
    web_scraper_order = 1L,
    course_url        = "https://guiadocent.urv.cat/guido/public/assignatura/ABC123",
    coordinators      = "Prof. Coordinador 1"
  )
  
  course_professors <- tibble::tibble(
    web_scraper_order = 1L,
    course_url        = "https://guiadocent.urv.cat/guido/public/assignatura/ABC123",
    professors        = "Prof. Docent 1"
  )
  
  course_description <- tibble::tibble(
    web_scraper_order = c(1L, 2L),
    course_url        = c(
      "https://guiadocent.urv.cat/guido/public/assignatura/ABC123",
      "https://guiadocent.urv.cat/guido/public/assignatura/ABC123"
    ),
    description       = c("Descripció part 1", "Descripció part 2")
  )
  
  course_contents <- tibble::tibble(
    web_scraper_order = 1L,
    course_url        = "https://guiadocent.urv.cat/guido/public/assignatura/ABC123",
    contents          = "Continguts del curs"
  )
  
  course_clr <- tibble::tibble(
    web_scraper_order             = 1L,
    course_url                    = "https://guiadocent.urv.cat/guido/public/assignatura/ABC123",
    competences_learning_results  = "Competències i resultats"
  )
  
  course_bibliography <- tibble::tibble(
    web_scraper_order = 1L,
    course_url        = "https://guiadocent.urv.cat/guido/public/assignatura/ABC123",
    references        = "Referència 1"
  )
  
  guides_raw <- list(
    degree_programs                     = degree_programs,
    course_details                      = course_details,
    course_coordinators                 = course_coordinators,
    course_professors                   = course_professors,
    course_description                  = course_description,
    course_contents                     = course_contents,
    course_competences_learning_results = course_clr,
    course_bibliography                 = course_bibliography
  )
  
  # Act -----------------------------------------------------------------------
  index <- build_guides_index_from_raw(guides_raw)
  
  # Basic shape ---------------------------------------------------------------
  expect_s3_class(index, "data.frame")
  expect_equal(nrow(index), 1L)
  
  # Document number should be sequential starting at 1 ------------------------
  expect_equal(index$document_number, 1L)
  
  # Degree year extracted from parentheses ------------------------------------
  expect_equal(index$degree_year, "2020")
  
  # Faculty name inferred from faculty_school_url -----------------------------
  expect_equal(
    index$faculty_school_name,
    "Facultat de Turisme i Geografia"
  )
  
  # Aggregated long text fields -----------------------------------------------
  # Description has two rows joined with " ; "
  expect_equal(
    index$description,
    "Descripció part 1 ; Descripció part 2"
  )
  
  # Contents, competences and references preserved
  expect_equal(index$contents, "Continguts del curs")
  expect_equal(index$competences_learning_results, "Competències i resultats")
  expect_equal(index$references, "Referència 1")
  
  # Key identifiers copied correctly ------------------------------------------
  expect_equal(index$course_code, "ABC123")
  expect_equal(index$course_name, "Introducció al turisme")
  expect_equal(index$degree_name, "Grau en Turisme (2020)")
})
