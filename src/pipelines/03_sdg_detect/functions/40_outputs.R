# File: src/pipelines/03_sdg_detect/functions/40_outputs.R
# Purpose:
#   SDG detection (text2sdg) - reviewer-friendly outputs.
#
# Contains:
#   - build_guides_sdg_summary(): one row per course with global + per-section SDG fields
#   - build_guides_sdg_review(): one row per detected SDG (course repeats), for manual review
#
# Additions (this refactor):
#   - Optionally attach feature-based summaries derived from `features_long`.
#     (No assumptions about any future visualization target.)

build_guides_sdg_summary <- function(guides_translated, sdg_hits_long, sdg_cfg, features_long = NULL) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_summary(): guides_translated must contain document_number.", call. = FALSE)
  }
  
  if (!nrow(guides_translated)) {
    return(guides_translated)
  }
  
  out <- guides_translated |>
    dplyr::distinct(document_number, .keep_all = TRUE)
  
  # ------------------------------------------------------------------
  # SDG-level summaries (existing behavior)
  # ------------------------------------------------------------------
  if (!nrow(sdg_hits_long)) {
    out <- out |>
      dplyr::mutate(
        sdg_any        = FALSE,
        sdg_n_distinct = 0L,
        sdg_sdgs       = "",
        sdg_n_systems  = 0L,
        sdg_systems    = "",
        sdg_total_hits = 0L
      )
  } else {
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
    
    out <- out |>
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
  
  # ------------------------------------------------------------------
  # Feature-level summaries (new, optional)
  # ------------------------------------------------------------------
  if (!is.null(features_long) && nrow(features_long)) {
    # We aggregate features in two ways:
    #   1) Global per-document stats + top list (features_*)
    #   2) Per-section wide columns (features_*_<section>)
    features_counts <- summarise_features_long(features_long)
    
    feat_doc <- summarise_features_per_document(
      features_counts,
      top_n = as.integer(sdg_cfg$features_top_n %||% 30L),
      sep   = sdg_cfg$features_sep %||% ";\n",
      terms_sep = sdg_cfg$features_terms_sep %||% ", "
    )
    
    feat_by_section <- summarise_features_per_document_section(
      features_counts,
      top_n = as.integer(sdg_cfg$features_top_n %||% 30L),
      sep   = sdg_cfg$features_sep %||% ";\n"
    )
    
    out <- out |>
      dplyr::left_join(feat_doc, by = "document_number") |>
      dplyr::left_join(feat_by_section, by = "document_number") |>
      dplyr::mutate(
        features_n_distinct = dplyr::coalesce(as.integer(features_n_distinct), 0L),
        features_total      = dplyr::coalesce(as.integer(features_total), 0L),
        features_top        = dplyr::coalesce(features_top, "")
      )
  } else {
    # Provide stable empty columns so downstream export code is simpler.
    out <- out |>
      dplyr::mutate(
        features_n_distinct = 0L,
        features_total      = 0L,
        features_top        = ""
      )
  }
  
  out
}

build_guides_sdg_review <- function(guides_translated, sdg_hits_long, sdg_cfg, features_long = NULL) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_review(): guides_translated must contain document_number.", call. = FALSE)
  }
  
  if (!nrow(sdg_hits_long)) {
    return(
      tibble::tibble(
        document_number = character(0),
        sdg_id          = character(0),
        sections        = character(0),
        systems         = character(0),
        total_hits      = integer(0)
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
      systems_section = collapse_unique_text(system, sep = ", "),
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
  
  out <- global |>
    dplyr::left_join(by_section, by = c("document_number", "sdg_id"))
  
  if (!is.null(labels_tbl) && nrow(labels_tbl)) {
    out <- out |>
      dplyr::left_join(labels_tbl, by = "sdg_id")
  }
  
  # Optional: attach per (document, sdg) feature summaries (compact string)
  if (!is.null(features_long) && nrow(features_long)) {
    feat_sdg <- features_long |>
      dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
      dplyr::count(document_number, sdg_id, feature, name = "n") |>
      dplyr::arrange(document_number, sdg_id, dplyr::desc(n), feature) |>
      dplyr::group_by(document_number, sdg_id) |>
      dplyr::slice_head(n = as.integer(sdg_cfg$features_top_n %||% 30L)) |>
      dplyr::summarise(
        features_top = paste0(feature, collapse = sdg_cfg$features_sep %||% ";\n"),
        .groups = "drop"
      )
    
    out <- out |>
      dplyr::left_join(feat_sdg, by = c("document_number", "sdg_id")) |>
      dplyr::mutate(features_top = dplyr::coalesce(features_top, ""))
  }
  
  out
}
