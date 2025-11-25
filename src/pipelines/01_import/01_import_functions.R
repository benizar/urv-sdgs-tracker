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
# NOTE:
#   - This file intentionally keeps "raw" structures:
#     no text cleaning / normalisation is performed here.
#   - All joins are explicit and we avoid base::merge() to reduce
#     accidental row multiplication and column name surprises.

# -------------------------------------------------------------------
# Pipeline configuration loader
# -------------------------------------------------------------------

# Load the main pipeline YAML configuration.
load_pipeline_config <- function(path = "config/pipeline.yml") {
  if (!file.exists(path)) {
    stop("Pipeline config file not found: ", path)
  }
  yaml::read_yaml(path)
}

# -------------------------------------------------------------------
# Import directory resolution and data download
# -------------------------------------------------------------------

# Resolve the directory used as input for the import step.
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
ensure_import_data <- function(import_cfg) {
  root   <- import_cfg$root %||% "data"
  dir    <- import_cfg$dir
  subdir <- import_cfg$subdir
  
  if (is.null(dir) || !nzchar(dir)) {
    stop("import$dir is not set in config/pipeline.yml.")
  }
  
  base_dir <- file.path(root, dir)
  import_dir <- if (is.null(subdir) || !nzchar(subdir)) base_dir else file.path(base_dir, subdir)
  
  # Case 1: import_dir already exists -> use it as-is.
  if (dir.exists(import_dir)) {
    message("Using existing import directory: ", import_dir)
    return(import_dir)
  }
  
  # Case 2: import_dir does not exist. If repo root does not exist, download it.
  if (!dir.exists(base_dir)) {
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
    
    if (!dir.exists(root)) {
      dir.create(root, recursive = TRUE, showWarnings = FALSE)
    }
    
    is_zip <- isTRUE(import_cfg$zip %||% TRUE)
    tmp    <- tempfile(fileext = if (is_zip) ".zip" else "")
    
    utils::download.file(url, tmp, mode = "wb")
    message("Downloaded archive to: ", tmp)
    
    if (is_zip) {
      utils::unzip(tmp, exdir = root)
      message("Unzipped archive into: ", root)
    } else {
      dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
      file.copy(tmp, file.path(base_dir, basename(url)), overwrite = TRUE)
      message("Copied downloaded file into: ", base_dir)
    }
    
    unlink(tmp)
  }
  
  if (!dir.exists(base_dir)) {
    stop(
      "Repository root does not exist after download/unzip: ", base_dir,
      "\nCheck the archive structure or adjust import$dir in config/pipeline.yml."
    )
  }
  
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
# Small IO helpers (kept local to this file)
# -------------------------------------------------------------------

read_csv_quiet <- function(path) {
  readr::read_csv(path, show_col_types = FALSE)
}

read_delim_semicolon_utf8 <- function(path) {
  readr::read_delim(
    path,
    delim = ";",
    escape_double = FALSE,
    locale = readr::locale(encoding = "UTF-8"),
    trim_ws = TRUE,
    show_col_types = FALSE
  )
}

rename_exact <- function(df, new_names) {
  if (length(new_names) != ncol(df)) {
    stop(
      "rename_exact(): expected ", length(new_names),
      " columns but found ", ncol(df), "."
    )
  }
  names(df) <- new_names
  df
}

# DOCNET iframe mapping:
#   - Expected columns include: "web-scraper-start-url" and an iframe column.
#   - The iframe column name can vary ("iframes", "iframes-href", ...).
#   - Returns a table: course_url (start url) -> iframe_url (or NA).
read_docnet_iframes <- function(import_dir) {
  path <- file.path(import_dir, "docnet-iframe.csv")
  if (!file.exists(path)) {
    warning("DOCNET iframe file not found: ", path)
    return(tibble::tibble(course_url = character(0), iframe_url = character(0)))
  }
  
  raw <- read_delim_semicolon_utf8(path)
  
  if (!"web-scraper-start-url" %in% names(raw)) {
    stop(
      "docnet-iframe.csv is missing column 'web-scraper-start-url'.\n",
      "Available columns: ", paste(names(raw), collapse = ", ")
    )
  }
  
  # Be tolerant to column naming differences in the scraper output.
  iframe_col <- intersect(
    names(raw),
    c("iframes", "iframes-href", "iframe", "iframe_url", "iframes_href")
  )[1]
  
  if (is.na(iframe_col)) {
    stop(
      "docnet-iframe.csv does not contain a recognised iframe column.\n",
      "Available columns: ", paste(names(raw), collapse = ", ")
    )
  }
  
  raw %>%
    dplyr::transmute(
      course_url = .data[["web-scraper-start-url"]],
      iframe_url = .data[[iframe_col]]
    ) %>%
    dplyr::mutate(
      iframe_url = dplyr::na_if(iframe_url, "")
    ) %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      # If duplicates exist, keep the first non-empty iframe URL.
      iframe_url = dplyr::first(iframe_url[!is.na(iframe_url)]),
      .groups = "drop"
    )
}

# Read + standardise course details (GUIdO).
read_course_details_guido <- function(import_dir) {
  df <- read_csv_quiet(file.path(import_dir, "course_details_list_guido.csv"))
  
  df <- rename_exact(df, c(
    "web_scraper_order",
    "degree_url",
    "course_code", "course_name", "course_delivery_mode",
    "course_url",
    "course_period", "course_type", "credits", "year"
  ))
  
  df$source_system <- "guido"
  df
}

# Read + standardise course details (DOCNET), replacing course_url by iframe when present.
read_course_details_docnet <- function(import_dir) {
  df <- read_csv_quiet(file.path(import_dir, "course_details_list_docnet.csv"))
  
  df <- rename_exact(df, c(
    "web_scraper_order", "degree_url",
    "course_delivery_mode", "course_code", "course_name", "course_url",
    "course_period", "course_type", "credits", "year"
  ))
  
  ifr <- read_docnet_iframes(import_dir)
  
  df <- df %>%
    dplyr::left_join(ifr, by = "course_url") %>%
    dplyr::mutate(
      course_url = dplyr::if_else(
        !is.na(iframe_url) & iframe_url != "",
        iframe_url,
        course_url
      )
    ) %>%
    dplyr::select(-iframe_url)
  
  df$source_system <- "docnet"
  df
}

# Helper to read and unify GUIdO/DOCNET "long text" tables with the same schema.
read_course_text_pair <- function(import_dir, guido_file, docnet_file, value_col) {
  g <- read_csv_quiet(file.path(import_dir, guido_file))
  d <- read_csv_quiet(file.path(import_dir, docnet_file))
  
  g <- rename_exact(g, c("web_scraper_order", "course_url", value_col))
  d <- rename_exact(d, c("web_scraper_order", "course_url", value_col))
  
  g <- g %>% dplyr::select(web_scraper_order, course_url, dplyr::all_of(value_col))
  d <- d %>% dplyr::select(web_scraper_order, course_url, dplyr::all_of(value_col))
  
  dplyr::bind_rows(g, d)
}

# -------------------------------------------------------------------
# CSV-based loader for guides data
# -------------------------------------------------------------------

# Read raw guide tables from a single import_dir and return a named list.
build_guides_raw <- function(import_dir) {
  if (!dir.exists(import_dir)) {
    stop("Import directory does not exist: ", import_dir)
  }
  
  message("Reading raw guides data from: ", import_dir)
  
  # ------------------------------
  # Degree programs list.
  # ------------------------------
  degree_programs_list <- read_delim_semicolon_utf8(
    file.path(import_dir, "degree_programs_list.csv")
  )
  degree_programs_list <- rename_exact(degree_programs_list, c(
    "web_scraper_order", "faculty_school_url",
    "degree_name", "degree_url"
  ))
  
  # ------------------------------
  # Course details (GUIdO + DOCNET).
  # ------------------------------
  course_details_list_guido  <- read_course_details_guido(import_dir)
  course_details_list_docnet <- read_course_details_docnet(import_dir)
  
  # Harmonise column order explicitly.
  course_details_list <- dplyr::bind_rows(
    course_details_list_guido %>% dplyr::select(
      web_scraper_order, degree_url,
      course_url, course_code, course_name, course_delivery_mode,
      course_period, course_type, credits, year,
      source_system
    ),
    course_details_list_docnet %>% dplyr::select(
      web_scraper_order, degree_url,
      course_url, course_code, course_name, course_delivery_mode,
      course_period, course_type, credits, year,
      source_system
    )
  )
  
  # ------------------------------
  # Coordinators.
  # ------------------------------
  guido_course_coordinators <- read_csv_quiet(
    file.path(import_dir, "guido-course-coordinators.csv")
  )
  docnet_course_coordinators <- read_csv_quiet(
    file.path(import_dir, "docnet-course-coordinators.csv")
  )
  
  guido_course_coordinators <- rename_exact(guido_course_coordinators, c(
    "web_scraper_order", "course_url",
    "coordinators", "coordinators_url"
  ))
  docnet_course_coordinators <- rename_exact(docnet_course_coordinators, c(
    "web_scraper_order", "course_url",
    "coordinators"
  ))
  
  course_coordinators <- dplyr::bind_rows(
    guido_course_coordinators %>% dplyr::select(web_scraper_order, course_url, coordinators),
    docnet_course_coordinators %>% dplyr::select(web_scraper_order, course_url, coordinators)
  )
  
  # ------------------------------
  # Professors.
  # ------------------------------
  guido_course_professors <- read_csv_quiet(
    file.path(import_dir, "guido-course-professors.csv")
  )
  docnet_course_professors <- read_csv_quiet(
    file.path(import_dir, "docnet-course-professors.csv")
  )
  
  guido_course_professors <- rename_exact(guido_course_professors, c(
    "web_scraper_order", "course_url",
    "professors", "professors_url"
  ))
  docnet_course_professors <- rename_exact(docnet_course_professors, c(
    "web_scraper_order", "course_url",
    "professors"
  ))
  
  course_professors <- dplyr::bind_rows(
    guido_course_professors %>% dplyr::select(web_scraper_order, course_url, professors),
    docnet_course_professors %>% dplyr::select(web_scraper_order, course_url, professors)
  )
  
  # ------------------------------
  # Description / contents / bibliography.
  # ------------------------------
  course_description <- read_course_text_pair(
    import_dir,
    guido_file = "guido-course-description.csv",
    docnet_file = "docnet-course-description.csv",
    value_col = "description"
  )
  
  course_contents <- read_course_text_pair(
    import_dir,
    guido_file = "guido-course-contents.csv",
    docnet_file = "docnet-course-contents.csv",
    value_col = "contents"
  )
  
  course_bibliography <- read_course_text_pair(
    import_dir,
    guido_file = "guido-course-bibliography.csv",
    docnet_file = "docnet-course-bibliography.csv",
    value_col = "references"
  )
  
  # ------------------------------
  # Learning results (GUIdO + DOCNET).
  # ------------------------------
  course_learning_results <- read_course_text_pair(
    import_dir,
    guido_file = "guido-course-learning-results.csv",
    docnet_file = "docnet-course-learning-results.csv",
    value_col = "competences_learning_results"
  )
  
  # ------------------------------
  # Competences (DOCNET only).
  # ------------------------------
  docnet_course_competences <- read_csv_quiet(
    file.path(import_dir, "docnet-course-competences.csv")
  )
  docnet_course_competences <- rename_exact(docnet_course_competences, c(
    "web_scraper_order", "course_url",
    "competences_learning_results"
  ))
  
  # Union of learning results + competences.
  course_competences_learning_results <- dplyr::bind_rows(
    course_learning_results,
    docnet_course_competences %>%
      dplyr::select(web_scraper_order, course_url, competences_learning_results)
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
build_guides_index_from_raw <- function(guides_raw) {
  degree_programs <- guides_raw$degree_programs
  course_details  <- guides_raw$course_details
  course_coord    <- guides_raw$course_coordinators
  course_prof     <- guides_raw$course_professors
  course_desc     <- guides_raw$course_description
  course_contents <- guides_raw$course_contents
  course_clr      <- guides_raw$course_competences_learning_results
  course_bib      <- guides_raw$course_bibliography
  
  # Normalise degree_url so that GUIdO and DOCNET match.
  degree_programs <- degree_programs %>%
    dplyr::mutate(degree_url = sub("/detall$", "", degree_url))
  
  course_details <- course_details %>%
    dplyr::mutate(degree_url = sub("/assignatures/gestionar$", "", degree_url))
  
  # Aggregate long text fields by course_url (no cleaning yet).
  description_agg <- course_desc %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(description = paste(description, collapse = " ; "), .groups = "drop")
  
  contents_agg <- course_contents %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(contents = paste(contents, collapse = " ; "), .groups = "drop")
  
  clr_agg <- course_clr %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(
      competences_learning_results = paste(competences_learning_results, collapse = " ; "),
      .groups = "drop"
    )
  
  bibliography_agg <- course_bib %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(references = paste(references, collapse = " ; "), .groups = "drop")
  
  # Aggregate coordinators and professors by course_url.
  coordinators_agg <- course_coord %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(coordinators = paste(unique(coordinators), collapse = " ; "), .groups = "drop")
  
  professors_agg <- course_prof %>%
    dplyr::group_by(course_url) %>%
    dplyr::summarise(professors = paste(unique(professors), collapse = " ; "), .groups = "drop")
  
  # Join everything into a single per-course table.
  guides_index <- course_details %>%
    dplyr::left_join(description_agg,  by = "course_url") %>%
    dplyr::left_join(contents_agg,     by = "course_url") %>%
    dplyr::left_join(clr_agg,          by = "course_url") %>%
    dplyr::left_join(bibliography_agg, by = "course_url") %>%
    dplyr::left_join(coordinators_agg, by = "course_url") %>%
    dplyr::left_join(professors_agg,   by = "course_url") %>%
    dplyr::left_join(
      degree_programs %>% dplyr::select(-web_scraper_order),
      by = "degree_url"
    ) %>%
    dplyr::mutate(
      faculty_school_name = assign_faculty_name(degree_url)
    ) %>%
    dplyr::arrange(
      faculty_school_name, degree_name, course_name, course_code
    ) %>%
    dplyr::mutate(
      document_number = dplyr::row_number(),
      degree_year = stringr::str_extract(degree_name, "(?<=\\()\\d{4}(?=\\))")
    ) %>%
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
      source_system,
      dplyr::everything(),
      -web_scraper_order
    )
  
  guides_index
}

# High-level entry point: obtain raw guides data from the config.
get_guides_raw <- function(import_cfg) {
  import_dir <- ensure_import_data(import_cfg)
  build_guides_raw(import_dir)
}

# High-level entry point: directly return the per-course index.
get_guides_index <- function(import_cfg) {
  import_dir   <- ensure_import_data(import_cfg)
  guides_raw   <- build_guides_raw(import_dir)
  build_guides_index_from_raw(guides_raw)
}
