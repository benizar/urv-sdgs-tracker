# File: src/pipelines/01_load/01_load_functions.R
# Load functions for reading guides data from a single data batch.
#
# NOTE:
#   - Structural joins are performed explicitly in build_guides_index_from_loaded().
#   - base::merge() is avoided to reduce accidental row multiplication.
#   - DOCnet competences + learning results are fused into:
#       course_competences_and_results
#   - DOCnet course_code includes optional modality suffix letter (v/s/p...).

# -------------------------------------------------------------------
# Column order conventions
# -------------------------------------------------------------------

course_info_col_order <- c(
  "course_coordinators",
  "course_professors",
  "course_description",
  "course_contents",
  "course_competences_and_results",
  "course_references"
)

# -------------------------------------------------------------------
# Pipeline configuration loader
# -------------------------------------------------------------------

load_pipeline_config <- function(path = "config/pipeline.yml") {
  if (!file.exists(path)) stop("Pipeline config file not found: ", path)
  yaml::read_yaml(path)
}

# -------------------------------------------------------------------
# Load directory resolution and data download
# -------------------------------------------------------------------

resolve_load_dir <- function(load_cfg) {
  root   <- load_cfg$root %||% "data"
  dir    <- load_cfg$dir
  subdir <- load_cfg$subdir
  
  if (is.null(dir) || !nzchar(dir)) stop("load$dir is not set in config/pipeline.yml.")
  if (is.null(subdir) || !nzchar(subdir)) return(file.path(root, dir))
  
  file.path(root, dir, subdir)
}

ensure_load_data <- function(load_cfg) {
  root           <- load_cfg$root %||% "data"
  dir            <- load_cfg$dir
  subdir         <- load_cfg$subdir
  url            <- load_cfg$url
  archive_path   <- load_cfg$archive_path %||% file.path(root, paste0(dir, ".zip"))
  force_download <- isTRUE(load_cfg$force_download %||% FALSE)
  is_zip         <- isTRUE(load_cfg$zip %||% TRUE)
  
  if (is.null(dir) || !nzchar(dir)) stop("load$dir is not set in config/pipeline.yml.")
  
  base_dir <- file.path(root, dir)
  load_dir <- if (is.null(subdir) || !nzchar(subdir)) base_dir else file.path(base_dir, subdir)
  
  if (dir.exists(load_dir)) {
    message("Using existing load directory: ", load_dir)
    return(load_dir)
  }
  
  if (!dir.exists(base_dir)) {
    if (is.null(url) || !nzchar(url)) {
      stop(
        "Load directory does not exist: ", load_dir,
        "\nRepository root does not exist either: ", base_dir,
        "\nNo 'url' provided in pipeline config under load$url, ",
        "so data cannot be downloaded automatically."
      )
    }
    
    if (!dir.exists(root)) dir.create(root, recursive = TRUE, showWarnings = FALSE)
    dir.create(dirname(archive_path), recursive = TRUE, showWarnings = FALSE)
    
    if (force_download || !file.exists(archive_path)) {
      message("Repository root not found (", base_dir, ").")
      message("Will download archive from: ", url)
      message("Saving archive to: ", archive_path)
      utils::download.file(url, archive_path, mode = "wb")
    } else {
      message("Repository root not found (", base_dir, ").")
      message("Using existing archive file: ", archive_path)
    }
    
    if (is_zip) {
      utils::unzip(archive_path, exdir = root)
      message("Unzipped archive into: ", root)
    } else {
      dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
      file.copy(archive_path, file.path(base_dir, basename(archive_path)), overwrite = TRUE)
      message("Copied downloaded file into: ", base_dir)
    }
  }
  
  if (!dir.exists(base_dir)) {
    stop(
      "Repository root does not exist after download/unzip: ", base_dir,
      "\nCheck the archive structure or adjust load$dir in config/pipeline.yml."
    )
  }
  
  if (!dir.exists(load_dir)) {
    stop(
      "Load directory not found after download/unzip: ", load_dir,
      "\nCheck that the archive contains this scraping subfolder or ",
      "adjust load$subdir in config/pipeline.yml."
    )
  }
  
  message("Using load directory: ", load_dir)
  load_dir
}

# -------------------------------------------------------------------
# Loader helpers
# -------------------------------------------------------------------

clean_course_info_table <- function(df,
                                    id_col,
                                    collapse = ";\n",
                                    col_sep  = " ") {
  if (!is.data.frame(df)) stop("`df` must be a data.frame.")
  if (!is.character(id_col) || length(id_col) != 1 || !nzchar(id_col)) {
    stop("`id_col` must be a non-empty character scalar.")
  }
  if (!id_col %in% names(df)) stop("`id_col` not found in df: ", id_col)
  
  html_cols <- grep("_html$", names(df), value = TRUE)
  cols_to_clean <- unique(c(html_cols, intersect("course_description", names(df))))
  
  out <- df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(cols_to_clean),
        ~ extract_text_from_html(.x, collapse = collapse, col_sep = col_sep, use_cache = TRUE)
      )
    )
  
  if ("course_coordinators_html" %in% names(out)) {
    out <- out |>
      dplyr::mutate(course_coordinators_html = normalize_name_list_cell_particles(course_coordinators_html))
  }
  if ("course_professors_html" %in% names(out)) {
    out <- out |>
      dplyr::mutate(course_professors_html = normalize_name_list_cell_particles(course_professors_html))
  }
  
  out |>
    dplyr::relocate(dplyr::all_of(id_col), .before = dplyr::everything()) |>
    dplyr::rename_with(~ sub("_html$", "", .x), .cols = dplyr::all_of(html_cols))
}

# -------------------------------------------------------------------
# Loader functions
# -------------------------------------------------------------------

get_centres_list <- function(load_dir, csv_name = "1_centres_list.csv") {
  filepath <- file.path(load_dir, csv_name)
  if (!file.exists(filepath)) stop("File does not exist: ", filepath)
  
  df <- readr::read_csv(filepath, show_col_types = FALSE)
  
  keep <- c("centre_name", "centre_name-href")
  missing <- setdiff(keep, names(df))
  if (length(missing) > 0) stop("Missing expected column(s): ", paste(missing, collapse = ", "))
  
  df |>
    dplyr::select(dplyr::all_of(keep)) |>
    dplyr::rename(centre_url = `centre_name-href`) |>
    dplyr::mutate(centre_code = stringr::str_match(centre_url, "/centres/(\\d+)/")[, 2]) |>
    dplyr::select(centre_code, dplyr::everything())
}

get_programmes_list <- function(load_dir, csv_name = "2_programmes_list.csv") {
  filepath <- file.path(load_dir, csv_name)
  if (!file.exists(filepath)) stop("File does not exist: ", filepath)
  
  df <- readr::read_csv(filepath, show_col_types = FALSE)
  
  keep <- c("web-scraper-start-url", "programme_name", "programme_name-href")
  missing <- setdiff(keep, names(df))
  if (length(missing) > 0) stop("Missing expected column(s): ", paste(missing, collapse = ", "))
  
  out <- df |>
    dplyr::select(dplyr::all_of(keep)) |>
    dplyr::rename(
      centre_url    = `web-scraper-start-url`,
      programme_url = `programme_name-href`
    ) |>
    dplyr::mutate(
      centre_code    = stringr::str_match(centre_url, "/centres/(\\d{3})/")[, 2],
      programme_code = stringr::str_match(programme_url, "/ensenyaments/(\\d+)/")[, 2],
      centre_programme_code = paste0(centre_code, "_", programme_code)
    ) |>
    dplyr::relocate(centre_url, centre_code, .before = dplyr::everything()) |>
    dplyr::relocate(programme_url, programme_code, .after = programme_name) |>
    dplyr::relocate(centre_programme_code, .after = programme_code)
  
  if (anyNA(out$centre_code)) stop("Could not extract `centre_code` from one or more `centre_url` values.")
  if (anyNA(out$programme_code)) stop("Could not extract `programme_code` from one or more `programme_url` values.")
  
  out
}

get_course_details_list <- function(load_dir, csv_name = "3_course_details_list.csv") {
  filepath <- file.path(load_dir, csv_name)
  if (!file.exists(filepath)) stop("File does not exist: ", filepath)
  
  df <- readr::read_csv(filepath, show_col_types = FALSE)
  
  keep <- c(
    "web-scraper-start-url",
    "academic_year",
    "course_name",
    "course_link-href",
    "course_period",
    "course_type",
    "course_credits",
    "course_external_info_html"
  )
  
  missing <- setdiff(keep, names(df))
  if (length(missing) > 0) stop("Missing expected column(s): ", paste(missing, collapse = ", "))
  
  out <- df |>
    dplyr::select(dplyr::all_of(keep)) |>
    dplyr::rename(
      programme_url = `web-scraper-start-url`,
      course_url    = `course_link-href`
    ) |>
    dplyr::mutate(
      source_system = dplyr::case_when(
        is.na(course_url) | !nzchar(course_url) ~ "External",
        stringr::str_detect(course_url, "docnet") ~ "DOCnet",
        TRUE ~ "GUIDO"
      ),
      centre_code    = stringr::str_match(programme_url, "/centres/(\\d{3})/")[, 2],
      programme_code = stringr::str_match(programme_url, "/ensenyaments/(\\d+)/")[, 2],
      centre_programme_code = paste0(centre_code, "_", programme_code),
      
      # Course details list is treated as GUIdO-code (digits) index.
      course_code = dplyr::if_else(
        is.na(course_url) | !nzchar(course_url),
        NA_character_,
        stringr::str_match(course_url, "/assignatures/(\\d+)/")[, 2]
      ),
      centre_programme_course_code = dplyr::if_else(
        is.na(course_code),
        NA_character_,
        paste0(centre_code, "_", programme_code, "_", course_code)
      )
    ) |>
    dplyr::relocate(source_system, .before = dplyr::everything()) |>
    dplyr::relocate(programme_url, centre_code, programme_code, centre_programme_code, .before = course_name) |>
    dplyr::relocate(course_url, course_code, centre_programme_course_code, .after = course_name)
  
  if (anyNA(out$centre_code) || anyNA(out$programme_code)) {
    stop("Could not extract centre/programme codes from `programme_url` for one or more rows.")
  }
  
  bad <- which((is.na(out$course_url) | !nzchar(out$course_url)) &
                 (is.na(out$course_external_info_html) | !nzchar(out$course_external_info_html)))
  if (length(bad) > 0) {
    stop("Rows with missing `course_url` also have empty `course_external_info_html`: ", paste(bad, collapse = ", "))
  }
  
  out
}

get_guido_docnet_course_code_map <- function(load_dir, csv_name = "4_docnet_course_urls.csv") {
  filepath <- file.path(load_dir, csv_name)
  if (!file.exists(filepath)) stop("File does not exist: ", filepath)
  
  df <- readr::read_csv(filepath, show_col_types = FALSE)
  
  keep <- c("web-scraper-start-url", "docnet_course_url")
  missing <- setdiff(keep, names(df))
  if (length(missing) > 0) stop("Missing expected column(s): ", paste(missing, collapse = ", "))
  
  df |>
    dplyr::rename(guido_course_url = `web-scraper-start-url`) |>
    dplyr::mutate(
      guido  = purrr::map(guido_course_url,  parse_guido_course_url),
      docnet = purrr::map(docnet_course_url, parse_docnet_course_url)
    ) |>
    tidyr::unnest_wider(guido,  names_sep = "_") |>
    tidyr::unnest_wider(docnet, names_sep = "_") |>
    dplyr::transmute(
      guido_centre_programme_course_code  = paste0(guido_centre_code,  "_", guido_programme_code,  "_", guido_course_code),
      docnet_centre_programme_course_code = paste0(docnet_centre_code, "_", docnet_programme_code, "_", docnet_course_code),
      docnet_modality = docnet_modality,
      guido_course_url = guido_course_url,
      docnet_course_url = docnet_course_url
    )
}

# -------------------------------------------------------------------
# Loader functions (DOCnet): clean + fuse competences/results
# -------------------------------------------------------------------

get_docnet_course_info <- function(load_dir,
                                   csv_name = "5_docnet_course_info.csv",
                                   collapse = ";\n",
                                   col_sep  = " ") {
  filepath <- file.path(load_dir, csv_name)
  if (!file.exists(filepath)) stop("File does not exist: ", filepath)
  
  df <- readr::read_csv(filepath, show_col_types = FALSE)
  
  required <- c(
    "web-scraper-start-url",
    "course_coordinators_html",
    "course_professors_html",
    "course_description",
    "course_contents_html",
    "course_competences_html",
    "course_learning_results_html",
    "course_references_html"
  )
  missing_req <- setdiff(required, names(df))
  if (length(missing_req) > 0) stop("Missing expected column(s): ", paste(missing_req, collapse = ", "))
  
  out <- df |>
    dplyr::rename(docnet_course_url = `web-scraper-start-url`) |>
    dplyr::mutate(
      docnet = purrr::map(docnet_course_url, parse_docnet_course_url)
    ) |>
    tidyr::unnest_wider(docnet, names_sep = "_") |>
    dplyr::mutate(
      docnet_centre_programme_course_code = paste0(docnet_centre_code, "_", docnet_programme_code, "_", docnet_course_code),
      docnet_modality = docnet_modality
    ) |>
    dplyr::select(
      docnet_course_url,
      docnet_centre_programme_course_code,
      docnet_modality,
      course_coordinators_html,
      course_professors_html,
      course_description,
      course_contents_html,
      course_competences_html,
      course_learning_results_html,
      course_references_html
    )
  
  out <- clean_course_info_table(
    df = out,
    id_col = "docnet_centre_programme_course_code",
    collapse = collapse,
    col_sep = col_sep
  )
  
  out <- fuse_columns_to_text(
    out,
    cols = c("course_competences", "course_learning_results"),
    out  = "course_competences_and_results",
    labels = c("Competències", "Resultats d'aprenentatge"),
    drop_inputs = TRUE
  )
  
  out |>
    dplyr::select(
      docnet_centre_programme_course_code,
      dplyr::any_of(course_info_col_order),
      docnet_course_url
    ) |>
    dplyr::as_tibble()
}

# -------------------------------------------------------------------
# Loader functions (GUIdO): clean + create competences/results fused column
# -------------------------------------------------------------------

get_guido_course_info <- function(load_dir,
                                  csv_name = "5_guido_course_info.csv",
                                  collapse = ";\n",
                                  col_sep  = " ") {
  filepath <- file.path(load_dir, csv_name)
  if (!file.exists(filepath)) stop("File does not exist: ", filepath)
  
  df <- readr::read_csv(filepath, show_col_types = FALSE)
  
  keep <- c(
    "web-scraper-start-url",
    "course_coordinators_html",
    "course_professors_html",
    "course_description",
    "course_learning_results_html",
    "course_contents_html",
    "course_references_html"
  )
  missing <- setdiff(keep, names(df))
  if (length(missing) > 0) stop("Missing expected column(s): ", paste(missing, collapse = ", "))
  
  out <- df |>
    dplyr::select(dplyr::all_of(keep)) |>
    dplyr::rename(guido_course_url = `web-scraper-start-url`) |>
    dplyr::mutate(
      guido = purrr::map(guido_course_url, parse_guido_course_url)
    ) |>
    tidyr::unnest_wider(guido, names_sep = "_") |>
    dplyr::mutate(
      guido_centre_programme_course_code = paste0(guido_centre_code, "_", guido_programme_code, "_", guido_course_code)
    )
  
  html_cols <- grep("_html$", names(out), value = TRUE)
  
  special_cols <- c(
    "course_coordinators_html",
    "course_professors_html",
    "course_learning_results_html",
    "course_references_html",
    "course_contents_html"
  )
  
  cols_to_clean_generic <- setdiff(unique(c(html_cols, "course_description")), special_cols)
  
  out <- out |>
    dplyr::mutate(
      course_coordinators_html = extract_anchor_text_list_from_html(course_coordinators_html, sep_out = collapse, use_cache = TRUE),
      course_professors_html   = extract_anchor_text_list_from_html(course_professors_html,   sep_out = collapse, use_cache = TRUE),
      
      course_contents_html = dplyr::coalesce(
        extract_block_items_from_html(course_contents_html, xpath = ".//p|.//li", collapse = collapse, use_cache = TRUE),
        extract_text_from_html(course_contents_html, collapse = collapse, col_sep = col_sep, use_cache = TRUE)
      ),
      course_contents_html = dplyr::na_if(course_contents_html, ""),
      
      course_learning_results_html = extract_guido_learning_results_from_html(course_learning_results_html, collapse = collapse, use_cache = TRUE),
      course_learning_results_html = dplyr::na_if(course_learning_results_html, ""),
      
      course_references_html = extract_reference_titles_from_html(course_references_html, collapse = collapse, use_cache = TRUE),
      course_references_html = dplyr::na_if(course_references_html, ""),
      
      dplyr::across(dplyr::all_of(cols_to_clean_generic),
                    ~ extract_text_from_html(.x, collapse = collapse, col_sep = col_sep, use_cache = TRUE)
      )
    ) |>
    dplyr::rename_with(~ sub("_html$", "", .x), .cols = dplyr::all_of(html_cols))
  
  if (!"course_competences" %in% names(out)) out$course_competences <- NA_character_
  if (!"course_learning_results" %in% names(out)) out$course_learning_results <- NA_character_
  
  out <- fuse_columns_to_text(
    out,
    cols = c("course_competences", "course_learning_results"),
    out  = "course_competences_and_results",
    labels = c("Competències", "Resultats d'aprenentatge"),
    drop_inputs = TRUE
  )
  
  out |>
    dplyr::select(
      guido_centre_programme_course_code,
      dplyr::any_of(course_info_col_order),
      guido_course_url
    ) |>
    dplyr::as_tibble()
}

# -------------------------------------------------------------------
# High-level entry point: build per-course index from loaded tables.
# (Step version: course master + URL normalization for DOCnet)
# -------------------------------------------------------------------

build_guides_index_from_loaded <- function(centres_list,
                                           programmes_list,
                                           course_details_list,
                                           guido_docnet_course_code_map,
                                           docnet_course_info,
                                           guido_course_info) {
  # 1) Base table (course details) + labels + mapping + canonical course_code + canonical course_url
  out <- course_details_list |>
    dplyr::filter(source_system != "External") |>
    dplyr::left_join(
      programmes_list |>
        dplyr::select(dplyr::all_of(c("centre_programme_code", "programme_name"))),
      by = "centre_programme_code"
    ) |>
    dplyr::left_join(
      centres_list |>
        dplyr::select(dplyr::all_of(c("centre_code", "centre_name"))),
      by = "centre_code"
    ) |>
    dplyr::left_join(
      guido_docnet_course_code_map |>
        dplyr::select(dplyr::all_of(c(
          "guido_centre_programme_course_code",
          "docnet_centre_programme_course_code",
          "docnet_course_url"
        ))),
      by = c("centre_programme_course_code" = "guido_centre_programme_course_code")
    ) |>
    dplyr::mutate(
      # canonical join key (DOCnet triple+modality, else GUIdO triple)
      course_code = dplyr::if_else(
        source_system == "DOCnet",
        docnet_centre_programme_course_code,
        centre_programme_course_code
      ),
      # canonical url for traceability (DOCnet rows use mapped docnet url)
      course_url = dplyr::if_else(
        source_system == "DOCnet",
        docnet_course_url,
        course_url
      )
    ) |>
    dplyr::select(
      source_system, academic_year,
      centre_name, programme_name,
      
      # codes you want to keep
      centre_programme_course_code,  # always GUIdO triple
      course_code,                   # canonical (GUIdO triple or DOCnet triple+modality)
      
      # only url you want to keep
      course_url,
      
      # course metadata
      course_name, course_period, course_type, course_credits
    ) |>
    dplyr::as_tibble()
  
  # 2) Stack info tables (rowbind) into a single table keyed by course_code
  guido_info2 <- guido_course_info |>
    dplyr::transmute(
      course_code = guido_centre_programme_course_code,
      dplyr::across(dplyr::starts_with("course_"))
    )
  
  docnet_info2 <- docnet_course_info |>
    dplyr::transmute(
      course_code = docnet_centre_programme_course_code,
      dplyr::across(dplyr::starts_with("course_"))
    )
  
  info_stacked <- dplyr::bind_rows(guido_info2, docnet_info2)
  
  # 3) Final join
  out |>
    dplyr::left_join(info_stacked, by = "course_code") |>
    dplyr::as_tibble()
}
