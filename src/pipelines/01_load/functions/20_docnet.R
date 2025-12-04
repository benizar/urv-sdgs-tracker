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
