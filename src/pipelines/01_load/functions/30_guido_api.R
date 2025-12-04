# -------------------------------------------------------------------
# Loader functions
# -------------------------------------------------------------------


course_info_col_order <- c(
  "course_coordinators",
  "course_professors",
  "course_description",
  "course_contents",
  "course_competences_and_results",
  "course_references"
)

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

extract_guido_learning_results_from_html <- function(html_text,
                                                     collapse = ";\n",
                                                     use_cache = TRUE) {
  # Extract GUIdO learning results as an ordered, de-duplicated list:
  # - Keep parent competences (SE1 - ...) without duplicating child items.
  # - Keep leaf learning outcomes (SE1.5 - ...) once.
  # - Preserve order and join with `collapse`.
  
  parse_one <- function(x) {
    if (is.na(x) || !nzchar(x)) return(NA_character_)
    
    # Wrap to ensure a single root node for HTML parsing.
    doc <- xml2::read_html(paste0("<div>", x, "</div>"))
    
    # Parent nodes: take ONLY direct text nodes (avoid dragging nested <li> text).
    parent_nodes <- xml2::xml_find_all(
      doc,
      ".//li[contains(@class,'li_competencia')] | .//li[contains(@class,'li_competencia_especifica')]"
    )
    
    parent_txt <- vapply(parent_nodes, function(li) {
      tn <- xml2::xml_find_all(li, "./text()")
      txt <- paste(xml2::xml_text(tn), collapse = " ")
      txt <- gsub("[ \t\r\n]+", " ", txt, perl = TRUE)
      trimws(txt)
    }, character(1))
    
    parent_txt <- parent_txt[nzchar(parent_txt)]
    
    # Leaf items: learning outcomes that do not contain other <li> descendants.
    leaf_nodes <- xml2::xml_find_all(doc, ".//li[not(descendant::li)]")
    leaf_txt <- xml2::xml_text(leaf_nodes)
    leaf_txt <- gsub("[ \t\r\n]+", " ", leaf_txt, perl = TRUE)
    leaf_txt <- trimws(leaf_txt)
    leaf_txt <- leaf_txt[nzchar(leaf_txt)]
    
    items <- c(parent_txt, leaf_txt)
    
    # Drop duplicates whilst preserving the first occurrence.
    items <- items[!duplicated(items)]
    
    if (!length(items)) return(NA_character_)
    paste(items, collapse = collapse)
  }
  
  if (use_cache) {
    uniq <- unique(html_text)
    idx  <- match(html_text, uniq)
    uniq_out <- vapply(uniq, parse_one, character(1))
    return(uniq_out[idx])
  }
  
  vapply(html_text, parse_one, character(1))
}
