# File: src/pipelines/03_sdg_detect/functions/40_outputs.R
# Purpose:
#   SDG detection (text2sdg) - reviewer-friendly outputs.
#
# Contains:
#   - build_guides_sdg_summary(): one row per course with global + per-section SDG fields
#   - build_guides_sdg_review(): one row per detected SDG (course repeats), for manual review
#
# Notes:
#   - Uses sdg_to_id() (10_input.R).
#   - Uses collapse_unique_text() from common for compact lists.
#   - Optionally reads SDG labels from sdg_cfg$labels_csv (default resources/sdg_labels.csv)
#     via read_sdg_labels() if available.

build_guides_sdg_summary <- function(guides_translated, sdg_hits_long, sdg_cfg) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_summary(): guides_translated must contain document_number.", call. = FALSE)
  }
  
  if (!nrow(guides_translated)) {
    return(guides_translated)
  }
  
  out <- guides_translated |>
    dplyr::distinct(document_number, .keep_all = TRUE)
  
  if (!nrow(sdg_hits_long)) {
    return(
      out |>
        dplyr::mutate(
          sdg_any        = FALSE,
          sdg_n_distinct = 0L,
          sdg_sdgs       = "",
          sdg_n_systems  = 0L,
          sdg_systems    = "",
          sdg_total_hits = 0L
        )
    )
  }
  
  global <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number) |>
    dplyr::summarise(
      sdg_any        = TRUE,
      sdg_n_distinct = dplyr::n_distinct(sdg_id),
      sdg_sdgs       = collapse_unique_text(sdg_id, sep = ", "),
      sdg_n_systems  = dplyr::n_distinct(system),
      sdg_systems    = collapse_unique_text(system, sep = ", "),
      sdg_total_hits = sum(n_hits, na.rm = TRUE),
      .groups = "drop"
    )
  
  by_section <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number, section) |>
    dplyr::summarise(
      n_distinct = dplyr::n_distinct(sdg_id),
      sdgs       = collapse_unique_text(sdg_id,  sep = ", "),
      n_systems  = dplyr::n_distinct(system),
      systems    = collapse_unique_text(system,  sep = ", "),
      total_hits = sum(n_hits, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      id_cols = document_number,
      names_from = section,
      values_from = c(n_distinct, sdgs, n_systems, systems, total_hits),
      names_glue = "sdg_{.value}_{section}"
    )
  
  out |>
    dplyr::left_join(global, by = "document_number") |>
    dplyr::left_join(by_section, by = "document_number") |>
    dplyr::mutate(
      sdg_any        = dplyr::coalesce(sdg_any, FALSE),
      sdg_n_distinct = dplyr::coalesce(as.integer(sdg_n_distinct), 0L),
      sdg_sdgs       = dplyr::coalesce(sdg_sdgs, ""),
      sdg_n_systems  = dplyr::coalesce(as.integer(sdg_n_systems), 0L),
      sdg_systems    = dplyr::coalesce(sdg_systems, ""),
      sdg_total_hits = dplyr::coalesce(as.integer(sdg_total_hits), 0L)
    )
}

build_guides_sdg_review <- function(guides_translated, sdg_hits_long, sdg_cfg) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_review(): guides_translated must contain document_number.", call. = FALSE)
  }
  
  if (!nrow(sdg_hits_long)) {
    return(
      tibble::tibble(
        document_number = character(0),
        sdg_id = character(0),
        sections = character(0),
        systems = character(0),
        total_hits = integer(0)
      )
    )
  }
  
  global <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number, sdg_id) |>
    dplyr::summarise(
      sections   = collapse_unique_text(section, sep = ", "),
      systems    = collapse_unique_text(system,  sep = ", "),
      total_hits = sum(n_hits, na.rm = TRUE),
      .groups = "drop"
    )
  
  by_section <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number, sdg_id, section) |>
    dplyr::summarise(
      hits_section    = sum(n_hits, na.rm = TRUE),
      systems_section = collapse_unique_text(system,  sep = ", "),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      id_cols = c(document_number, sdg_id),
      names_from = section,
      values_from = c(hits_section, systems_section),
      names_glue = "{.value}_{section}"
    )
  
  labels_path <- sdg_cfg$labels_csv %||% "resources/sdg_labels.csv"
  labels_tbl  <- tryCatch(read_sdg_labels(labels_path), error = function(e) NULL)
  
  get_len <- function(df, col) {
    if (!col %in% names(df)) return(rep(0L, nrow(df)))
    stringr::str_length(dplyr::coalesce(as.character(df[[col]]), ""))
  }
  
  meta <- guides_translated |>
    dplyr::mutate(
      course_description_len = get_len(dplyr::pick(dplyr::everything()), "course_description_en"),
      course_contents_len    = get_len(dplyr::pick(dplyr::everything()), "course_contents_en"),
      course_competences_len = get_len(dplyr::pick(dplyr::everything()), "course_competences_and_results_en"),
      course_references_len  = get_len(dplyr::pick(dplyr::everything()), "course_references_en")
    ) |>
    dplyr::distinct(document_number, .keep_all = TRUE) |>
    dplyr::select(
      document_number,
      tidyselect::ends_with("_len")
    )
  
  out <- global |>
    dplyr::left_join(by_section, by = c("document_number", "sdg_id"))
  
  if (!is.null(labels_tbl) && nrow(labels_tbl)) {
    out <- out |>
      dplyr::left_join(labels_tbl, by = "sdg_id")
  }
  
  out <- out |>
    dplyr::inner_join(meta, by = "document_number")
  
  if ("ods_label_ca" %in% names(out)) {
    out <- out |>
      dplyr::relocate(ods_label_ca, .after = sdg_id)
  }
  
  out
}
