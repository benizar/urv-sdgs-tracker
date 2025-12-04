# File: src/pipelines/03_sdg_detect/functions/30_summaries.R
# Purpose:
#   SDG detection (text2sdg) - post-processing helpers.
#
# Contains:
#   - summarise_sdg_hits_long(): one row per (document_number, section, system, sdg)
#   - summarise_sdg_hits_wide(): wide matrix for modeling / exports
#   - attach_sdg_to_guides(): attach wide SDG fields back to guides_translated
#
#   - summarise_features_long(): one row per (document_number, section, system, sdg, feature)
#   - summarise_features_per_document(): compact per-document feature stats + top terms
#   - summarise_features_per_document_section(): same but per section (wide columns)

# -------------------------------------------------------------------
# SDG hits summaries
# -------------------------------------------------------------------

summarise_sdg_hits_long <- function(hits) {
  if (!nrow(hits)) {
    return(
      tibble::tibble(
        document_number = character(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        n_hits          = integer(0),
        sdg_num         = character(0)
      )
    )
  }
  
  hits |>
    dplyr::mutate(
      sdg = as.character(sdg),
      sdg_num = stringr::str_pad(
        stringr::str_extract(sdg, "\\d+"),
        width = 2,
        pad = "0"
      )
    ) |>
    dplyr::group_by(document_number, section, system, sdg, sdg_num) |>
    dplyr::summarise(n_hits = dplyr::n(), .groups = "drop")
}

summarise_sdg_hits_wide <- function(sdg_long, sdg_cfg) {
  if (!nrow(sdg_long)) {
    return(tibble::tibble(document_number = character(0)))
  }
  
  agg_mode <- sdg_cfg$aggregate$mode %||% "counts"
  min_hits <- as.integer(sdg_cfg$aggregate$min_hits %||% 1L)
  if (is.na(min_hits) || min_hits < 1L) min_hits <- 1L
  
  agg_mode <- tolower(agg_mode)
  if (!agg_mode %in% c("counts", "binary")) {
    stop("summarise_sdg_hits_wide(): invalid aggregate.mode: ", agg_mode, call. = FALSE)
  }
  
  sdg_long |>
    dplyr::mutate(
      value = if (identical(agg_mode, "binary")) as.integer(n_hits >= min_hits) else n_hits,
      colname = paste0("sdg_", sdg_num, "_", system, "_", section)
    ) |>
    dplyr::select(document_number, colname, value) |>
    tidyr::pivot_wider(
      id_cols     = document_number,
      names_from  = colname,
      values_from = value,
      values_fill = 0
    )
}

attach_sdg_to_guides <- function(guides_translated, sdg_hits_wide) {
  guides_translated |>
    dplyr::left_join(sdg_hits_wide, by = "document_number")
}

# -------------------------------------------------------------------
# Features summaries
# -------------------------------------------------------------------

summarise_features_long <- function(features_long) {
  if (!nrow(features_long)) {
    return(
      tibble::tibble(
        document_number = character(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        sdg_num         = character(0),
        feature         = character(0),
        n               = integer(0)
      )
    )
  }
  
  needed <- c("document_number", "section", "system", "sdg", "feature")
  if (!all(needed %in% names(features_long))) {
    stop(
      "summarise_features_long(): features_long must contain columns: ",
      paste(needed, collapse = ", "),
      call. = FALSE
    )
  }
  
  features_long |>
    dplyr::mutate(
      sdg = as.character(sdg),
      sdg_num = stringr::str_pad(
        stringr::str_extract(sdg, "\\d+"),
        width = 2,
        pad = "0"
      ),
      feature = trimws(as.character(feature))
    ) |>
    dplyr::filter(!is.na(feature), feature != "") |>
    dplyr::count(document_number, section, system, sdg, sdg_num, feature, name = "n", sort = TRUE)
}

summarise_features_per_document <- function(features_counts, top_n = 30, sep = ";\n", terms_sep = ", ") {
  if (!nrow(features_counts)) {
    return(
      tibble::tibble(
        document_number       = character(0),
        features_n_distinct   = integer(0),
        features_total        = integer(0),
        features_terms        = character(0),
        features_top          = character(0)
      )
    )
  }
  
  by_doc <- features_counts |>
    dplyr::group_by(document_number, feature) |>
    dplyr::summarise(n = sum(n, na.rm = TRUE), .groups = "drop") |>
    dplyr::arrange(document_number, dplyr::desc(n), feature)
  
  stats <- by_doc |>
    dplyr::group_by(document_number) |>
    dplyr::summarise(
      features_n_distinct = dplyr::n_distinct(feature),
      features_total      = as.integer(sum(n, na.rm = TRUE)),
      features_terms      = collapse_unique_text(feature, sep = terms_sep),
      .groups = "drop"
    )
  
  top <- by_doc |>
    dplyr::group_by(document_number) |>
    dplyr::slice_head(n = top_n) |>
    dplyr::summarise(
      features_top = paste0(feature, collapse = sep),
      .groups = "drop"
    )
  
  stats |>
    dplyr::left_join(top, by = "document_number") |>
    dplyr::mutate(
      features_terms = dplyr::coalesce(features_terms, ""),
      features_top   = dplyr::coalesce(features_top, "")
    )
}

summarise_features_per_document_section <- function(features_counts, top_n = 30, sep = ";\n") {
  if (!nrow(features_counts)) {
    return(tibble::tibble(document_number = character(0)))
  }
  
  if (!all(c("document_number", "section", "feature", "n") %in% names(features_counts))) {
    stop(
      "summarise_features_per_document_section(): expected columns: document_number, section, feature, n.",
      call. = FALSE
    )
  }
  
  by_doc_sec <- features_counts |>
    dplyr::group_by(document_number, section, feature) |>
    dplyr::summarise(n = sum(n, na.rm = TRUE), .groups = "drop") |>
    dplyr::arrange(document_number, section, dplyr::desc(n), feature)
  
  stats <- by_doc_sec |>
    dplyr::group_by(document_number, section) |>
    dplyr::summarise(
      features_n_distinct = dplyr::n_distinct(feature),
      features_total      = as.integer(sum(n, na.rm = TRUE)),
      .groups = "drop"
    )
  
  top <- by_doc_sec |>
    dplyr::group_by(document_number, section) |>
    dplyr::slice_head(n = top_n) |>
    dplyr::summarise(
      features_top = paste0(feature, collapse = sep),
      .groups = "drop"
    )
  
  out <- stats |>
    dplyr::left_join(top, by = c("document_number", "section")) |>
    dplyr::mutate(features_top = dplyr::coalesce(features_top, "")) |>
    tidyr::pivot_wider(
      id_cols = document_number,
      names_from = section,
      values_from = c(features_n_distinct, features_total, features_top),
      names_glue = "{.value}_{section}"
    )
  
  out
}
