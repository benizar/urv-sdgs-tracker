# File: src/pipelines/03_sdg_detect/functions/40_outputs.R
# Purpose:
#   SDG detection (text2sdg) - final pipeline outputs.
#
# Contains:
#   - build_guides_sdg_summary(): 1 row per course with per-section lists only:
#       - sdg_sdgs_<section>
#       - sdg_systems_<section>
#       - features_<section>
#   - build_guides_sdg_review(): 1 row per detected SDG (course repeats),
#       with section/system/feature lists (no hit counts).
#
# Notes:
#   - Avoids counts, totals, top-N.
#   - Requires collapse_unique_text() (src/common).

.build_section_ids <- function(sdg_cfg, sdg_hits_long) {
  ids <- character(0)
  
  if (!is.null(sdg_cfg$combine_groups)) {
    ids <- unlist(purrr::map(sdg_cfg$combine_groups, "id"))
  }
  
  ids <- as.character(ids)
  ids <- ids[!is.na(ids) & nzchar(ids)]
  
  if (!length(ids) &&
      !is.null(sdg_hits_long) && nrow(sdg_hits_long) &&
      "section" %in% names(sdg_hits_long)) {
    ids <- unique(as.character(sdg_hits_long$section))
    ids <- ids[!is.na(ids) & nzchar(ids)]
  }
  
  unique(ids)
}

.coalesce_chr <- function(x) {
  dplyr::coalesce(as.character(x), "")
}

# Robust SDG id normalisation: always "SDG-XX"
.normalize_sdg_id <- function(x) {
  if (is.null(x)) return(NA_character_)
  s <- as.character(x)
  s <- trimws(s)
  s[s == ""] <- NA_character_
  
  # Extract first number found
  num <- stringr::str_extract(s, "\\d+")
  num <- suppressWarnings(as.integer(num))
  
  out <- ifelse(is.na(num), NA_character_, paste0("SDG-", stringr::str_pad(num, 2, pad = "0")))
  out
}

# If previous runs created .x/.y columns, merge them back safely
.merge_xy_columns <- function(df, base_names) {
  for (nm in base_names) {
    xnm <- paste0(nm, ".x")
    ynm <- paste0(nm, ".y")
    
    has_x <- xnm %in% names(df)
    has_y <- ynm %in% names(df)
    has_b <- nm  %in% names(df)
    
    if (has_x && has_y) {
      df[[nm]] <- .coalesce_chr(df[[ynm]])
      # if .y empty, fallback to .x
      df[[nm]][df[[nm]] == ""] <- .coalesce_chr(df[[xnm]])[df[[nm]] == ""]
      df[[xnm]] <- NULL
      df[[ynm]] <- NULL
    } else if (!has_b && has_x) {
      names(df)[names(df) == xnm] <- nm
    } else if (!has_b && has_y) {
      names(df)[names(df) == ynm] <- nm
    }
    
    if (nm %in% names(df)) {
      df[[nm]] <- .coalesce_chr(df[[nm]])
    }
  }
  df
}

# -------------------------------------------------------------------
# build_guides_sdg_summary(): per-section lists only
# -------------------------------------------------------------------
build_guides_sdg_summary <- function(guides_translated, sdg_hits_long, sdg_cfg, features_long = NULL) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_summary(): guides_translated must contain document_number.", call. = FALSE)
  }
  if (!nrow(guides_translated)) return(guides_translated)
  
  out <- guides_translated |>
    dplyr::distinct(document_number, .keep_all = TRUE)
  
  list_sep     <- sdg_cfg$list_sep %||% ", "
  sdgs_sep     <- sdg_cfg$sdgs_sep %||% list_sep
  systems_sep  <- sdg_cfg$systems_sep %||% list_sep
  features_sep <- sdg_cfg$features_terms_sep %||% sdg_cfg$features_sep %||% list_sep
  
  section_ids <- .build_section_ids(sdg_cfg, sdg_hits_long)
  
  # -------------------------
  # SDG + system lists per section
  # -------------------------
  if (!is.null(sdg_hits_long) && nrow(sdg_hits_long)) {
    hits2 <- sdg_hits_long |>
      dplyr::mutate(
        sdg_id  = .normalize_sdg_id(.data$sdg),
        section = as.character(.data$section),
        system  = as.character(.data$system)
      ) |>
      dplyr::filter(!is.na(.data$sdg_id), nzchar(.data$sdg_id)) |>
      dplyr::filter(!is.na(.data$section), nzchar(.data$section)) |>
      dplyr::filter(!is.na(.data$system), nzchar(.data$system))
    
    by_section <- hits2 |>
      dplyr::group_by(.data$document_number, .data$section) |>
      dplyr::summarise(
        sdgs    = collapse_unique_text(.data$sdg_id,  sep = sdgs_sep),
        systems = collapse_unique_text(.data$system, sep = systems_sep),
        .groups = "drop"
      ) |>
      tidyr::pivot_wider(
        id_cols = .data$document_number,
        names_from = .data$section,
        values_from = c(sdgs, systems),
        names_glue = "sdg_{.value}_{section}"
      )
    
    out <- out |>
      dplyr::left_join(by_section, by = "document_number")
  }
  
  # Ensure expected sdg_*_<section> columns exist and are clean (and fix legacy .x/.y)
  if (length(section_ids)) {
    want_sdgs <- paste0("sdg_sdgs_", section_ids)
    want_sys  <- paste0("sdg_systems_", section_ids)
    
    out <- .merge_xy_columns(out, c(want_sdgs, want_sys))
    
    for (nm in c(want_sdgs, want_sys)) {
      if (!nm %in% names(out)) out[[nm]] <- ""
      out[[nm]] <- .coalesce_chr(out[[nm]])
    }
  }
  
  # -------------------------
  # Feature lists per section
  # -------------------------
  if (!is.null(features_long) && nrow(features_long)) {
    feat2 <- features_long |>
      dplyr::mutate(
        section = as.character(.data$section),
        feature = tolower(trimws(as.character(.data$feature)))
      ) |>
      dplyr::filter(!is.na(.data$section), nzchar(.data$section)) |>
      dplyr::filter(!is.na(.data$feature), nzchar(.data$feature))
    
    feat_by_section <- feat2 |>
      dplyr::group_by(.data$document_number, .data$section) |>
      dplyr::summarise(
        features = collapse_unique_text(.data$feature, sep = features_sep),
        .groups = "drop"
      ) |>
      tidyr::pivot_wider(
        id_cols = .data$document_number,
        names_from = .data$section,
        values_from = .data$features,
        names_glue = "features_{section}"
      )
    
    out <- out |>
      dplyr::left_join(feat_by_section, by = "document_number")
  }
  
  # Ensure expected features_<section> columns exist and are clean (and fix legacy .x/.y)
  if (length(section_ids)) {
    want_feat <- paste0("features_", section_ids)
    
    out <- .merge_xy_columns(out, want_feat)
    
    for (nm in want_feat) {
      if (!nm %in% names(out)) out[[nm]] <- ""
      out[[nm]] <- .coalesce_chr(out[[nm]])
    }
  }
  
  out
}

# -------------------------------------------------------------------
# build_guides_sdg_review(): 1 row per (document_number, sdg_id)
# -------------------------------------------------------------------
build_guides_sdg_review <- function(guides_translated, sdg_hits_long, sdg_cfg, features_long = NULL) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_review(): guides_translated must contain document_number.", call. = FALSE)
  }
  
  list_sep     <- sdg_cfg$list_sep %||% ", "
  systems_sep  <- sdg_cfg$systems_sep %||% list_sep
  features_sep <- sdg_cfg$features_terms_sep %||% sdg_cfg$features_sep %||% list_sep
  
  if (is.null(sdg_hits_long) || !nrow(sdg_hits_long)) {
    return(tibble::tibble(
      document_number = character(0),
      sdg_id          = character(0),
      sections        = character(0),
      systems         = character(0),
      features        = character(0)
    ))
  }
  
  hits2 <- sdg_hits_long |>
    dplyr::mutate(
      sdg_id  = .normalize_sdg_id(.data$sdg),
      section = as.character(.data$section),
      system  = as.character(.data$system)
    ) |>
    dplyr::filter(!is.na(.data$sdg_id), nzchar(.data$sdg_id))
  
  out <- hits2 |>
    dplyr::group_by(.data$document_number, .data$sdg_id) |>
    dplyr::summarise(
      sections = collapse_unique_text(.data$section, sep = list_sep),
      systems  = collapse_unique_text(.data$system,  sep = systems_sep),
      .groups = "drop"
    )
  
  # Attach features per (document, sdg)
  if (!is.null(features_long) && nrow(features_long)) {
    feat2 <- features_long |>
      dplyr::mutate(
        sdg_id  = .normalize_sdg_id(.data$sdg),
        feature = tolower(trimws(as.character(.data$feature)))
      ) |>
      dplyr::filter(!is.na(.data$sdg_id), nzchar(.data$sdg_id)) |>
      dplyr::filter(!is.na(.data$feature), nzchar(.data$feature))
    
    feat_sdg <- feat2 |>
      dplyr::group_by(.data$document_number, .data$sdg_id) |>
      dplyr::summarise(
        features = collapse_unique_text(.data$feature, sep = features_sep),
        .groups = "drop"
      )
    
    out <- out |>
      dplyr::left_join(feat_sdg, by = c("document_number", "sdg_id"))
  }
  
  out |>
    dplyr::mutate(
      sections = .coalesce_chr(.data$sections),
      systems  = .coalesce_chr(.data$systems),
      features = .coalesce_chr(.data$features)
    )
}
