# File: src/pipelines/01_import/01_import_functions.R
# Import functions for reading raw guides data from a single scraping batch.
#
# CURRENT BEHAVIOUR:
#   - Reads CSV files from a folder selected via config/scraping.yml.
#   - The folder can be either:
#       * sandbox/data/guides_scraping_<run_id>
#       * data/scrapings_archive/guides_scraping_<run_id>
#     depending on input_location ("sandbox" or "archive").
#
# FUTURE BEHAVIOUR:
#   - This module can be extended to use an internal R-based scraper
#     while preserving the same high-level output structure returned
#     by get_guides_raw(cfg).

# Decide the directory to use as input for the import step
# based on config/scraping.yml.
#
# This function returns a path like:
#   - sandbox/data/guides_scraping_<run_id>
#   - data/scrapings_archive/guides_scraping_<run_id>
# and stops with a clear error message if the directory does not exist.
resolve_import_dir <- function(cfg) {
  loc      <- cfg$input_location %||% "archive"
  run_id   <- cfg$run_id
  sandbox  <- cfg$sandbox_base_dir   %||% "sandbox/data"
  archive  <- cfg$archive_base_dir   %||% "data/scrapings_archive"

  if (is.null(run_id)) {
    stop("config/scraping.yml: 'run_id' is not set.")
  }

  if (loc == "archive") {
    dir_name <- file.path(archive, paste0("guides_scraping_", run_id))
  } else if (loc == "sandbox") {
    dir_name <- file.path(sandbox, paste0("guides_scraping_", run_id))
  } else {
    stop("Unknown input_location in config: ", loc)
  }

  if (!dir.exists(dir_name)) {
    stop(
      "Import directory does not exist: ", dir_name,
      "\nCheck config/scraping.yml (input_location, run_id) or ",
      "create/copy the archived scraping there."
    )
  }

  message("Using import directory: ", dir_name)
  dir_name
}

# Temporary CSV-based loader for guides scraping batches.
#
# This function reads all the CSV files exported from an external
# scraping process (for example, the "Web Scraper" browser plugin)
# from a single folder (import_dir) and returns a named list of
# raw data frames.
#
# Cleaning, translation and aggregation will be handled in later
# pipeline phases (02_clean, 03_translate, etc.).
build_guides_raw <- function(import_dir) {
  if (!dir.exists(import_dir)) {
    stop("Import directory does not exist: ", import_dir)
  }

  message("Reading raw guides data from: ", import_dir)

  # ------------------------------
  # Degree programs list.
  # ------------------------------
  degree_programs_list <- readr::read_delim(
    file.path(import_dir, "degree_programs_list.csv"),
    delim = ";",
    escape_double = FALSE,
    locale = readr::locale(encoding = "WINDOWS-1252"),
    trim_ws = TRUE
  )

  colnames(degree_programs_list) <- c(
    "web_scraper_order", "faculty_school_url",
    "degree_name", "degree_url"
  )

  # ------------------------------
  # Course details (GUIdO).
  # ------------------------------
  course_details_list_guido <- readr::read_csv(
    file.path(import_dir, "course_details_list_guido.csv"),
    show_col_types = FALSE
  )

  colnames(course_details_list_guido) <- c(
    "web_scraper_order",
    "degree_url",
    "course_code", "course_name", "course_delivery_mode",
    "course_url",
    "course_period", "course_type", "credits", "year"
  )

  # ------------------------------
  # Course details (DOCNET) + iframes.
  # ------------------------------
  course_details_list_docnet <- readr::read_csv(
    file.path(import_dir, "course_details_list_docnet.csv"),
    show_col_types = FALSE
  )

  colnames(course_details_list_docnet) <- c(
    "web_scraper_order", "degree_url",
    "course_delivery_mode", "course_code", "course_name", "course_url",
    "course_period", "course_type", "credits", "year"
  )

  docnet_iframes <- readr::read_delim(
    file.path(import_dir, "docnet-iframe.csv"),
    delim = ";",
    escape_double = FALSE,
    locale = readr::locale(encoding = "WINDOWS-1252"),
    trim_ws = TRUE
  )[ , c("web-scraper-start-url", "iframes")]

  course_details_list_docnet <- merge(
    course_details_list_docnet,
    docnet_iframes,
    by.x = "course_url",
    by.y = "web-scraper-start-url",
    all = TRUE
  )

  course_details_list_docnet <- subset(
    course_details_list_docnet,
    select = -c(course_url)
  )

  colnames(course_details_list_docnet) <- c(
    "web_scraper_order", "degree_url",
    "course_delivery_mode", "course_code", "course_name",
    "course_period", "course_type", "credits", "year",
    "course_url"
  )

  # Harmonise GUIdO and DOCNET course details.
  course_details_list_docnet <- course_details_list_docnet[ , c(
    "web_scraper_order", "degree_url",
    "course_url", "course_code", "course_name", "course_delivery_mode",
    "course_period", "course_type", "credits", "year"
  )]

  course_details_list_guido <- course_details_list_guido[ , c(
    "web_scraper_order", "degree_url",
    "course_url", "course_code", "course_name", "course_delivery_mode",
    "course_period", "course_type", "credits", "year"
  )]

  course_details_list <- rbind(
    course_details_list_guido,
    course_details_list_docnet
  )

  # ------------------------------
  # Course coordinators.
  # ------------------------------
  guido_course_coordinators <- readr::read_csv(
    file.path(import_dir, "guido-course-coordinators.csv"),
    show_col_types = FALSE
  )

  docnet_course_coordinators <- readr::read_csv(
    file.path(import_dir, "docnet-course-coordinators.csv"),
    show_col_types = FALSE
  )

  colnames(guido_course_coordinators) <- c(
    "web_scraper_order", "course_url",
    "coordinators", "coordinators_url"
  )

  colnames(docnet_course_coordinators) <- c(
    "web_scraper_order", "course_url",
    "coordinators"
  )

  guido_course_coordinators <- guido_course_coordinators[ , c(
    "web_scraper_order", "course_url", "coordinators"
  )]

  docnet_course_coordinators <- docnet_course_coordinators[ , c(
    "web_scraper_order", "course_url", "coordinators"
  )]

  course_coordinators <- rbind(
    guido_course_coordinators,
    docnet_course_coordinators
  )

  # ------------------------------
  # Course professors.
  # ------------------------------
  guido_course_professors <- readr::read_csv(
    file.path(import_dir, "guido-course-professors.csv"),
    show_col_types = FALSE
  )

  docnet_course_professors <- readr::read_csv(
    file.path(import_dir, "docnet-course-professors.csv"),
    show_col_types = FALSE
  )

  colnames(guido_course_professors) <- c(
    "web_scraper_order", "course_url",
    "professors", "professors_url"
  )

  colnames(docnet_course_professors) <- c(
    "web_scraper_order", "course_url",
    "professors"
  )

  guido_course_professors <- guido_course_professors[ , c(
    "web_scraper_order", "course_url", "professors"
  )]

  docnet_course_professors <- docnet_course_professors[ , c(
    "web_scraper_order", "course_url", "professors"
  )]

  course_professors <- rbind(
    guido_course_professors,
    docnet_course_professors
  )

  # ------------------------------
  # Course description.
  # ------------------------------
  guido_course_description <- readr::read_csv(
    file.path(import_dir, "guido-course-description.csv"),
    show_col_types = FALSE
  )

  docnet_course_description <- readr::read_csv(
    file.path(import_dir, "docnet-course-description.csv"),
    show_col_types = FALSE
  )

  colnames(guido_course_description) <- c(
    "web_scraper_order", "course_url",
    "description"
  )

  colnames(docnet_course_description) <- c(
    "web_scraper_order", "course_url",
    "description"
  )

  guido_course_description <- guido_course_description[ , c(
    "web_scraper_order", "course_url", "description"
  )]

  docnet_course_description <- docnet_course_description[ , c(
    "web_scraper_order", "course_url", "description"
  )]

  course_description <- rbind(
    guido_course_description,
    docnet_course_description
  )

  # ------------------------------
  # Course contents.
  # ------------------------------
  guido_course_contents <- readr::read_csv(
    file.path(import_dir, "guido-course-contents.csv"),
    show_col_types = FALSE
  )

  docnet_course_contents <- readr::read_csv(
    file.path(import_dir, "docnet-course-contents.csv"),
    show_col_types = FALSE
  )

  colnames(guido_course_contents) <- c(
    "web_scraper_order", "course_url",
    "contents"
  )

  colnames(docnet_course_contents) <- c(
    "web_scraper_order", "course_url",
    "contents"
  )

  guido_course_contents <- guido_course_contents[ , c(
    "web_scraper_order", "course_url", "contents"
  )]

  docnet_course_contents <- docnet_course_contents[ , c(
    "web_scraper_order", "course_url", "contents"
  )]

  course_contents <- rbind(
    guido_course_contents,
    docnet_course_contents
  )

  # ------------------------------
  # Learning results.
  # ------------------------------
  guido_course_learning_results <- readr::read_csv(
    file.path(import_dir, "guido-course-learning-results.csv"),
    show_col_types = FALSE
  )

  docnet_course_learning_results <- readr::read_csv(
    file.path(import_dir, "docnet-course-learning-results.csv"),
    show_col_types = FALSE
  )

  colnames(guido_course_learning_results) <- c(
    "web_scraper_order", "course_url",
    "competences_learning_results"
  )

  colnames(docnet_course_learning_results) <- c(
    "web_scraper_order", "course_url",
    "competences_learning_results"
  )

  guido_course_learning_results <- guido_course_learning_results[ , c(
    "web_scraper_order", "course_url", "competences_learning_results"
  )]

  docnet_course_learning_results <- docnet_course_learning_results[ , c(
    "web_scraper_order", "course_url", "competences_learning_results"
  )]

  course_learning_results <- rbind(
    guido_course_learning_results,
    docnet_course_learning_results
  )

  # ------------------------------
  # Competences (DOCNET only).
  # ------------------------------
  docnet_course_competences <- readr::read_csv(
    file.path(import_dir, "docnet-course-competences.csv"),
    show_col_types = FALSE
  )

  colnames(docnet_course_competences) <- c(
    "web_scraper_order", "course_url",
    "competences_learning_results"
  )

  course_competences <- docnet_course_competences

  # Union of learning results and competences.
  course_competences_learning_results <- rbind(
    course_learning_results,
    course_competences
  )

  # ------------------------------
  # Bibliography.
  # ------------------------------
  guido_course_bibliography <- readr::read_csv(
    file.path(import_dir, "guido-course-bibliography.csv"),
    show_col_types = FALSE
  )

  docnet_course_bibliography <- readr::read_csv(
    file.path(import_dir, "docnet-course-bibliography.csv"),
    show_col_types = FALSE
  )

  colnames(guido_course_bibliography) <- c(
    "web_scraper_order", "course_url",
    "references"
  )

  colnames(docnet_course_bibliography) <- c(
    "web_scraper_order", "course_url",
    "references"
  )

  guido_course_bibliography <- guido_course_bibliography[ , c(
    "web_scraper_order", "course_url", "references"
  )]

  docnet_course_bibliography <- docnet_course_bibliography[ , c(
    "web_scraper_order", "course_url", "references"
  )]

  course_bibliography <- rbind(
    guido_course_bibliography,
    docnet_course_bibliography
  )

  # ------------------------------
  # Return all raw tables as a named list.
  # ------------------------------
  list(
    degree_programs                      = degree_programs_list,
    course_details                       = course_details_list,
    course_coordinators                  = course_coordinators,
    course_professors                    = course_professors,
    course_description                   = course_description,
    course_contents                      = course_contents,
    course_competences_learning_results  = course_competences_learning_results,
    course_bibliography                  = course_bibliography
  )
}

# High-level entry point for obtaining raw guides data.
#
# CURRENT BEHAVIOUR:
#   - Thin wrapper around build_guides_raw(import_dir),
#     where import_dir is resolved from config/scraping.yml.
#
# FUTURE BEHAVIOUR:
#   - This function can be switched to call an internal scraper
#     (for example, run_guides_scraper(cfg)) while keeping the same
#     return structure: a named list of raw data frames.
get_guides_raw <- function(cfg) {
  import_dir <- resolve_import_dir(cfg)
  build_guides_raw(import_dir)
}

# Map a degree_url to a faculty_school_name.
#
# This keeps all the URL → faculty-name logic in a single place so that
# both the pipeline and the tests can rely on the same mapping.
assign_faculty_name <- function(url) {
  dplyr::case_when(
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/504/", url) ~
      "Escola Tècnica Superior d'Arquitectura",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/499/", url) ~
      "Escola Tècnica Superior d'Enginyeria",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/502/", url) ~
      "Escola Tècnica Superior d'Enginyeria Química",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/498/", url) ~
      "Facultat d'Economia i Empresa",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/501/", url) ~
      "Facultat d'Enologia",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/500/", url) ~
      "Facultat d'Infermeria",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/493/", url) ~
      "Facultat de Ciències de l'Educació i Psicologia",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/497/", url) ~
      "Facultat de Ciències Jurídiques",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/494/", url) ~
      "Facultat de Lletres",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/496/", url) ~
      "Facultat de Medicina i Ciències de la Salut",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/495/", url) ~
      "Facultat de Química",
    grepl("^https://guiadocent\\.urv\\.cat/guido/public/centres/503/", url) ~
      "Facultat de Turisme i Geografia",
    TRUE ~ NA_character_
  )
}


# Build a per-course index (one row per course_url) from the raw tables.
#
# This function only does structural joins and aggregations:
#   - normalises degree_url so GUIdO and DOCNET match
#   - aggregates long text fields by course_url
#   - aggregates coordinators and professors
#   - joins degree metadata
#   - derives document_number and degree_year
#
# It DOES NOT:
#   - clean text (no gsub, no bullet removal, no case normalisation)
#   - translate text into English
build_guides_index_from_raw <- function(guides_raw) {
  degree_programs <- guides_raw$degree_programs
  course_details  <- guides_raw$course_details
  course_coord    <- guides_raw$course_coordinators
  course_prof     <- guides_raw$course_professors
  course_desc     <- guides_raw$course_description
  course_contents <- guides_raw$course_contents
  course_clr      <- guides_raw$course_competences_learning_results
  course_bib      <- guides_raw$course_bibliography
  
  # 1) Normalise degree_url so that GUIdO and DOCNET match
  degree_programs <- degree_programs %>%
    dplyr::mutate(
      degree_url = sub("/detall$", "", degree_url)
    )
  
  course_details <- course_details %>%
    dplyr::mutate(
      degree_url = sub("/assignatures/gestionar$", "", degree_url)
    )
  
  # 2) Aggregate long text fields by course_url (no cleaning yet)
  description_agg <- course_desc %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      description = paste(description, collapse = " ; "),
      .groups = "drop"
    )
  
  contents_agg <- course_contents %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      contents = paste(contents, collapse = " ; "),
      .groups = "drop"
    )
  
  clr_agg <- course_clr %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      competences_learning_results =
        paste(competences_learning_results, collapse = " ; "),
      .groups = "drop"
    )
  
  bibliography_agg <- course_bib %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      references = paste(references, collapse = " ; "),
      .groups = "drop"
    )
  
  # 3) Aggregate coordinators and professors by course_url
  coordinators_agg <- course_coord %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      coordinators = paste(unique(coordinators), collapse = " ; "),
      .groups = "drop"
    )
  
  professors_agg <- course_prof %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      professors = paste(unique(professors), collapse = " ; "),
      .groups = "drop"
    )
  
  # 4) Join everything into a single per-course table
  guides_index <- course_details %>%
    # long texts
    dplyr::left_join(description_agg,  by = "course_url") %>%
    dplyr::left_join(contents_agg,     by = "course_url") %>%
    dplyr::left_join(clr_agg,          by = "course_url") %>%
    dplyr::left_join(bibliography_agg, by = "course_url") %>%
    # teaching staff
    dplyr::left_join(coordinators_agg, by = "course_url") %>%
    dplyr::left_join(professors_agg,   by = "course_url") %>%
    # degree metadata (brings faculty_school_url, degree_name, degree_url, etc.)
    dplyr::left_join(
      degree_programs %>% dplyr::select(-web_scraper_order),
      by = "degree_url"
    ) %>%
    # derive faculty_school_name from degree_url using the helper
    dplyr::mutate(
      faculty_school_name = assign_faculty_name(degree_url)
    ) %>%
    # sort before assigning document_number
    dplyr::arrange(
      faculty_school_name,
      degree_name,
      course_name,
      course_code
    ) %>%
    # derived fields
    dplyr::mutate(
      document_number = dplyr::row_number(),
      degree_year = stringr::str_extract(
        degree_name,
        "(?<=\\()\\d{4}(?=\\))"
      )
    ) %>%
    # put faculty first, then degrees, then courses, then everything else
    dplyr::select(
      document_number,
      faculty_school_name,
      faculty_school_url,
      degree_name,
      degree_year,
      degree_url,
      course_code,
      course_name,
      course_delivery_mode,
      course_period,
      course_type,
      credits,
      year,
      dplyr::everything(),
      -web_scraper_order
    )
  
  guides_index
}


# High-level entry point: directly return the per-course index
# from a config object (config/scraping.yml).
get_guides_index <- function(cfg) {
  import_dir  <- resolve_import_dir(cfg)
  guides_raw  <- build_guides_raw(import_dir)
  guides_index <- build_guides_index_from_raw(guides_raw)
  guides_index
}
