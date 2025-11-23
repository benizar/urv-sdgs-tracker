# File: src/pipelines/04_sdg/04_detect_sdg_functions.R
#
# SDG detection helpers for the URV SDGs tracker pipeline.
#
# Responsibilities:
#   - optionally rebuild a translated guides table from CSVs
#     (when sdg_detection$input_mode == "csv")
#   - build the SDG input table (document_number x section x text)
#   - run text2sdg (systems or ensemble)
#   - summarise hits (long and wide)
#   - attach SDG summaries back to the guides table
#
# NOTE:
#   - attach_translations_to_guides() is defined in
#     src/pipelines/03_translate/03_translate_functions.R
#     and must be sourced by _targets.R before these functions.
#   - translation CSVs are assumed to live in translate$output_dir
#     (e.g. "sandbox/translations") and follow the naming pattern
#     "<column>-<target_lang>-<service>.csv" with columns:
#       * id
#       * original
#       * detected_language
#       * confidence
#       * translated_text

# -------------------------------------------------------------------
# Load translation CSVs from disk (for input_mode == "csv")
# -------------------------------------------------------------------

load_translation_csvs <- function(translate_cfg, sdg_cfg = NULL) {
  # Decide directory for translation CSVs:
  # 1) sdg_detection$translation_dir (if provided)
  # 2) translate$output_dir
  output_dir <- NULL
  
  if (!is.null(sdg_cfg) && !is.null(sdg_cfg$translation_dir)) {
    output_dir <- sdg_cfg$translation_dir
  }
  if (is.null(output_dir) || !nzchar(output_dir)) {
    output_dir <- translate_cfg$output_dir %||% "sandbox/translations"
  }
  
  if (!dir.exists(output_dir)) {
    warning(
      "load_translation_csvs(): translation directory does not exist: ",
      output_dir
    )
    return(list())
  }
  
  service     <- tolower(translate_cfg$service %||% "libretranslate")
  target_lang <- translate_cfg$target_lang %||% "en"
  
  # Columns that were translated in the translate phase:
  #   - if translate_cfg$columns is set, use that list
  #   - otherwise, fall back to the defaults
  if (!is.null(translate_cfg$columns)) {
    columns_cfg <- translate_cfg$columns
  } else {
    columns_cfg <- c(
      "course_name_clean",
      "description_clean",
      "contents_clean",
      "competences_learning_results_clean",
      "references_clean"
    )
  }
  
  translation_dfs <- list()
  
  for (col in columns_cfg) {
    file_path <- file.path(
      output_dir,
      paste0(col, "-", target_lang, "-", service, ".csv")
    )
    
    if (!file.exists(file_path)) {
      warning(
        "load_translation_csvs(): translation CSV not found for column '",
        col, "': ", file_path, " (skipping)."
      )
      next
    }
    
    tr_df <- readr::read_csv(file_path, show_col_types = FALSE)
    
    required_cols <- c("id", "translated_text")
    missing_cols  <- setdiff(required_cols, names(tr_df))
    if (length(missing_cols)) {
      warning(
        "load_translation_csvs(): CSV ", file_path,
        " is missing required columns: ",
        paste(missing_cols, collapse = ", "),
        ". Skipping this file."
      )
      next
    }
    
    translation_dfs[[col]] <- tr_df
  }
  
  translation_dfs
}

# -------------------------------------------------------------------
# Prepare guides table for SDG detection
#   - Either use guides_translated (in-memory)
#   - Or rebuild a translated table from CSVs
# -------------------------------------------------------------------

prepare_guides_for_sdg <- function(
    guides_clean,
    guides_translated,
    translate_cfg,
    sdg_cfg
) {
  mode <- sdg_cfg$input_mode %||% "object"
  
  if (identical(mode, "csv")) {
    message("prepare_guides_for_sdg(): using translation CSVs from disk.")
    
    if (!exists("attach_translations_to_guides")) {
      stop(
        "attach_translations_to_guides() is not available.\n",
        "Make sure src/pipelines/03_translate/03_translate_functions.R ",
        "is sourced by _targets.R before SDG functions."
      )
    }
    
    translation_dfs <- load_translation_csvs(translate_cfg, sdg_cfg)
    
    if (!length(translation_dfs)) {
      warning(
        "prepare_guides_for_sdg(): no usable translation CSVs found. ",
        "Falling back to in-memory guides_translated."
      )
      return(guides_translated)
    }
    
    # Build a translated table from guides_clean + CSVs
    attach_translations_to_guides(guides_clean, translation_dfs)
  } else {
    message("prepare_guides_for_sdg(): using in-memory guides_translated.")
    guides_translated
  }
}

# -------------------------------------------------------------------
# Build SDG input table from a translated guides table and sdg config
# -------------------------------------------------------------------

# Output:
#   - one row per (document_number, section)
#   - columns: document_number, section, text
#
# Sections are defined in pipeline.yml under:
#   sdg_detection:
#     combine_groups:
#       - id: "course_info"
#         prefix: "Course information: "
#         columns: [ ... *_en ]
build_sdg_input <- function(guides_for_sdg, sdg_cfg) {
  combine_groups <- sdg_cfg$combine_groups
  
  # ------------------------------------------------------------------
  # Fallback: no combine_groups -> behave like a single "all_text" section
  # ------------------------------------------------------------------
  if (is.null(combine_groups) || !length(combine_groups)) {
    message(
      "build_sdg_input(): no combine_groups in config, ",
      "falling back to single 'all_text' section."
    )
    
    # Default *_en columns in the translated table
    default_cols <- c(
      "course_name_en",
      "description_en",
      "contents_en",
      "competences_learning_results_en",
      "references_en"
    )
    
    cols_existing <- intersect(default_cols, names(guides_for_sdg))
    if (!length(cols_existing)) {
      stop(
        "build_sdg_input(): no translatable columns found. ",
        "Expected at least one of: ",
        paste(default_cols, collapse = ", ")
      )
    }
    
    tmp <- guides_for_sdg |>
      dplyr::select(document_number, dplyr::all_of(cols_existing))
    
    text_vec <- apply(
      tmp[, cols_existing, drop = FALSE],
      1,
      function(row) {
        pieces <- trimws(as.character(row))
        pieces <- pieces[!is.na(pieces) & pieces != ""]
        paste(pieces, collapse = " ||| ")
      }
    )
    
    sdg_input <- tibble::tibble(
      document_number = guides_for_sdg$document_number,
      section         = "all_text",
      text            = text_vec
    ) |>
      dplyr::filter(!is.na(text) & text != "")
    
    return(sdg_input)
  }
  
  # ------------------------------------------------------------------
  # Normal path: one row per (document_number, combine_group$id)
  # ------------------------------------------------------------------
  out_list <- list()
  
  for (grp in combine_groups) {
    id     <- grp$id
    prefix <- grp$prefix %||% ""
    cols   <- grp$columns %||% character(0)
    
    if (is.null(id) || !nzchar(id)) {
      stop(
        "build_sdg_input(): combine_groups entries must include ",
        "a non-empty 'id' field."
      )
    }
    
    if (!length(cols)) {
      warning(
        "build_sdg_input(): combine_groups entry '", id,
        "' has no columns; skipping."
      )
      next
    }
    
    cols_existing <- intersect(cols, names(guides_for_sdg))
    missing_cols  <- setdiff(cols, cols_existing)
    
    if (length(missing_cols)) {
      warning(
        "build_sdg_input(): combine_groups '", id,
        "' refers to missing columns: ",
        paste(missing_cols, collapse = ", "),
        ". They will be ignored."
      )
    }
    
    if (!length(cols_existing)) {
      next
    }
    
    tmp <- guides_for_sdg |>
      dplyr::select(document_number, dplyr::all_of(cols_existing))
    
    text_vec <- apply(
      tmp[, cols_existing, drop = FALSE],
      1,
      function(row) {
        pieces <- trimws(as.character(row))
        pieces <- pieces[!is.na(pieces) & pieces != ""]
        if (!length(pieces)) return(NA_character_)
        paste(pieces, collapse = " ||| ")
      }
    )
    
    if (nzchar(prefix)) {
      text_vec <- ifelse(
        is.na(text_vec) | text_vec == "",
        NA_character_,
        paste0(prefix, text_vec)
      )
    }
    
    df_grp <- tibble::tibble(
      document_number = guides_for_sdg$document_number,
      section         = id,
      text            = text_vec
    ) |>
      dplyr::filter(!is.na(text) & text != "")
    
    out_list[[id]] <- df_grp
  }
  
  sdg_input <- dplyr::bind_rows(out_list)
  
  if (!nrow(sdg_input)) {
    stop(
      "build_sdg_input(): no non-empty text found for any combine_groups. ",
      "Check that the *_en columns referenced in combine_groups exist ",
      "and are not all empty."
    )
  }
  
  sdg_input
}

# -------------------------------------------------------------------
# Run text2sdg with configurable options
# -------------------------------------------------------------------

run_text2sdg_detection <- function(sdg_input, sdg_cfg) {
  method  <- sdg_cfg$method  %||% "systems"
  sdgs    <- sdg_cfg$sdgs    %||% 1:17
  verbose <- sdg_cfg$verbose %||% FALSE
  
  method <- tolower(method)
  
  if (identical(method, "systems")) {
    systems <- sdg_cfg$systems %||% c("Aurora", "Elsevier", "Auckland", "SIRIS")
    output  <- sdg_cfg$output  %||% "documents"
    
    # text2sdg::detect_sdg_systems() signature:
    #   detect_sdg_systems(text, system, sdgs, output, verbose, ...)
    hits <- text2sdg::detect_sdg_systems(
      text    = sdg_input$text,
      system  = systems,
      sdgs    = sdgs,
      output  = output,
      verbose = verbose
    )
    
  } else if (identical(method, "ensemble")) {
    # text2sdg::detect_sdg() signature:
    #   detect_sdg(text, sdgs, verbose, ...)
    hits <- text2sdg::detect_sdg(
      text    = sdg_input$text,
      sdgs    = sdgs,
      verbose = verbose
    )
    
  } else {
    stop("run_text2sdg_detection(): unknown method in config: ", method)
  }
  
  if (!nrow(hits)) {
    message("run_text2sdg_detection(): text2sdg returned 0 hits.")
    hits$document_number <- integer(0)
    hits$section         <- character(0)
    return(hits)
  }
  
  doc_index <- as.integer(hits$document)
  doc_map   <- sdg_input$document_number
  sec_map   <- sdg_input$section
  
  if (any(is.na(doc_index)) ||
      any(doc_index < 1L) ||
      any(doc_index > length(doc_map))) {
    stop("run_text2sdg_detection(): invalid 'document' indices in hits.")
  }
  
  hits %>%
    dplyr::mutate(
      document_number = doc_map[doc_index],
      section         = sec_map[doc_index]
    )
}

# -------------------------------------------------------------------
# Long summary: document_number x section x system x sdg
# -------------------------------------------------------------------

summarise_sdg_hits_long <- function(hits) {
  if (!nrow(hits)) {
    return(
      tibble::tibble(
        document_number = integer(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        n_hits          = integer(0),
        sdg_num         = character(0)
      )
    )
  }
  
  if (!"document_number" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'document_number' column is required.")
  }
  if (!"section" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'section' column is required.")
  }
  
  hits %>%
    dplyr::group_by(
      document_number,
      section,
      system,
      sdg
    ) %>%
    dplyr::summarise(
      n_hits = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      sdg_num = stringr::str_extract(sdg, "\\d+")
    )
}

# -------------------------------------------------------------------
# Wide summary: 1 row per document_number, 1 col per (section, system, sdg)
# -------------------------------------------------------------------

summarise_sdg_hits_wide <- function(sdg_long, sdg_cfg) {
  if (!nrow(sdg_long)) {
    return(
      tibble::tibble(
        document_number = integer(0)
      )
    )
  }
  
  agg_mode <- sdg_cfg$aggregate$mode     %||% "counts"
  min_hits <- sdg_cfg$aggregate$min_hits %||% 1L
  
  sdg_long %>%
    dplyr::mutate(
      value = if (identical(agg_mode, "binary")) {
        as.integer(n_hits >= min_hits)
      } else {
        n_hits
      },
      colname = paste0(
        "sdg_",
        sdg_num, "_",
        system, "_",
        section
      )
    ) %>%
    dplyr::select(
      document_number,
      colname,
      value
    ) %>%
    tidyr::pivot_wider(
      id_cols      = document_number,
      names_from   = colname,
      values_from  = value,
      values_fill  = 0
    )
}

# -------------------------------------------------------------------
# Attach SDG wide summary back to the guides table
# -------------------------------------------------------------------

attach_sdg_to_guides <- function(guides_for_sdg, sdg_hits_wide) {
  guides_for_sdg %>%
    dplyr::left_join(sdg_hits_wide, by = "document_number")
}
