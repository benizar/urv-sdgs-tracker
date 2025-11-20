# File: src/pipelines/01_import/01_import_functions.R
# Import functions for reading raw guides data from a single data batch.
#
# CURRENT BEHAVIOUR:
#   - Reads configuration from config/pipeline.yml.
#   - The `import` section controls:
#       * where the raw CSVs are stored locally (root/dir/subdir)
#       * which URL to use if the directory does not exist.
#
#   - If the local directory already exists:
#       * it is used as-is and no download is attempted.
#   - If it does NOT exist:
#       * the dataset is downloaded from the configured URL,
#         and unzipped into `root` if needed.
#       * the pipeline assumes the zip creates a top-level folder
#         named `dir` under `root`.
#
# FUTURE BEHAVIOUR:
#   - This module can be extended to support multiple scraping formats
#     by inspecting metadata files inside the repo (e.g. YAML/JSON in
#     root/dir and root/dir/subdir) and branching to format-specific
#     loaders.

# -------------------------------------------------------------------
# Pipeline configuration loader
# -------------------------------------------------------------------

# Load the main pipeline YAML configuration.
#
# By default, it reads config/pipeline.yml and returns a named list
# (as produced by yaml::read_yaml()).
load_pipeline_config <- function(path = "config/pipeline.yml") {
  if (!file.exists(path)) {
    stop("Pipeline config file not found: ", path)
  }
  yaml::read_yaml(path)
}

# -------------------------------------------------------------------
# Import directory resolution and data download
# -------------------------------------------------------------------

# Resolve the directory used as input for the import step
# based on the `import` section of pipeline_config.
#
# Supported fields in import_cfg:
#   - root  : base directory (e.g. "data")
#   - dir   : repository-level folder (e.g. "urv_guides_repo")
#   - subdir: scraping batch folder (e.g. "urv_guides_scraping_20250731")
#
# The final import directory is:
#   file.path(root, dir, subdir)
# If subdir is NULL or empty, it falls back to file.path(root, dir).
resolve_import_dir <- function(import_cfg) {
  root   <- import_cfg$root %||% "data"
  dir    <- import_cfg$dir
  subdir <- import_cfg$subdir
  
  if (is.null(dir) || !nzchar(dir)) {
    stop("import$dir is not set in config/pipeline.yml.")
  }
  
  if (is.null(subdir) || !nzchar(subdir)) {
    return(file.path(root, dir))
  }
  
  file.path(root, dir, subdir)
}

# Ensure the import directory exists and contains the data.
#
# Behaviour:
#   - Compute:
#       base_dir   = file.path(root, dir)
#       import_dir = file.path(root, dir, subdir)  (if subdir is set)
#   - If import_dir already exists:
#       * use it as-is (no download).
#   - Otherwise:
#       * if base_dir does NOT exist:
#           - download the archive from import_cfg$url
#           - if import_cfg$zip is TRUE (default), unzip into `root`
#             (assuming it creates a folder `dir` under `root`)
#           - otherwise, copy the downloaded file into base_dir
#       * after download/unzip, require import_dir to exist.
ensure_import_data <- function(import_cfg) {
  root   <- import_cfg$root %||% "data"
  dir    <- import_cfg$dir
  subdir <- import_cfg$subdir
  
  if (is.null(dir) || !nzchar(dir)) {
    stop("import$dir is not set in config/pipeline.yml.")
  }
  
  base_dir <- file.path(root, dir)
  import_dir <- if (is.null(subdir) || !nzchar(subdir)) {
    base_dir
  } else {
    file.path(base_dir, subdir)
  }
  
  # Case 1: import_dir already exists → use it as-is.
  if (dir.exists(import_dir)) {
    message("Using existing import directory: ", import_dir)
    return(import_dir)
  }
  
  # Case 2: import_dir does not exist. Check if repo root exists.
  if (!dir.exists(base_dir)) {
    # Need to download the repository archive.
    url <- import_cfg$url
    if (is.null(url) || !nzchar(url)) {
      stop(
        "Import directory does not exist: ", import_dir,
        "\nRepository root does not exist either: ", base_dir,
        "\nNo 'url' provided in pipeline config under import$url, ",
        "so data cannot be downloaded automatically."
      )
    }
    
    message("Repository root not found (", base_dir, ").")
    message("Will download archive from: ", url)
    
    # Ensure root directory exists.
    if (!dir.exists(root)) {
      dir.create(root, recursive = TRUE, showWarnings = FALSE)
    }
    
    # Temporary file for the download.
    is_zip <- isTRUE(import_cfg$zip %||% TRUE)
    tmp    <- tempfile(fileext = if (is_zip) ".zip" else "")
    
    utils::download.file(url, tmp, mode = "wb")
    message("Downloaded archive to: ", tmp)
    
    if (is_zip) {
      # Unzip into root. We assume the archive creates a top-level
      # folder with the name `dir` inside `root`.
      utils::unzip(tmp, exdir = root)
      message("Unzipped archive into: ", root)
    } else {
      # Non-zip: create base_dir and copy the file inside.
      dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
      file.copy(tmp, file.path(base_dir, basename(url)), overwrite = TRUE)
      message("Copied downloaded file into: ", base_dir)
    }
    
    unlink(tmp)
  }
  
  # At this point, base_dir should exist (either it was already there
  # or it was created by the download/unzip step).
  if (!dir.exists(base_dir)) {
    stop(
      "Repository root does not exist after download/unzip: ", base_dir,
      "\nCheck the archive structure or adjust import$dir in config/pipeline.yml."
    )
  }
  
  # If subdir is defined, we expect it to exist inside base_dir.
  if (!dir.exists(import_dir)) {
    stop(
      "Import directory not found after download/unzip: ", import_dir,
      "\nCheck that the archive contains this scraping subfolder or ",
      "adjust import$subdir in config/pipeline.yml."
    )
  }
  
  message("Using import directory: ", import_dir)
  import_dir
}

# -------------------------------------------------------------------
# CSV-based loader for guides data
# -------------------------------------------------------------------

# Temporary CSV-based loader for guides batches.
#
# This function reads all the CSV files exported from an external
# process (e.g. browser-based Web Scraper, or a prepared Zenodo repo)
# from a single folder (import_dir) and returns a named list of
# raw data frames.
#
# Cleaning, translation and aggregation are handled in later
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
    locale = readr::locale(encoding = "UTF-8"),
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

# -------------------------------------------------------------------
# High-level entry points
# -------------------------------------------------------------------

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

# High-level entry point: obtain raw guides data from the config.
#
# Expects import_cfg to be something like pipeline_config$import.
get_guides_raw <- function(import_cfg) {
  import_dir <- ensure_import_data(import_cfg)
  build_guides_raw(import_dir)
}

# High-level entry point: directly return the per-course index
# from the import section of the pipeline config.
get_guides_index <- function(import_cfg) {
  import_dir   <- ensure_import_data(import_cfg)
  guides_raw   <- build_guides_raw(import_dir)
  guides_index <- build_guides_index_from_raw(guides_raw)
  guides_index
}
