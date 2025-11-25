# File: src/pipelines/03_translate/03_translate_functions.R
# Translation helpers for the URV SDGs tracker pipeline.
#
# Responsibilities:
#   - call the common check_translation_service() helper to ensure
#     the translation service is reachable
#   - call the generic translate_column() helper from src/common
#     to produce per-column CSV files
#   - read those CSVs back and attach *_en columns to guides_clean
#
# NOTE:
# - check_translation_service() and rtrim_slash() are defined in
#   src/common/translation_helpers.R and loaded from _targets.R.
# - translate_column() is also defined in src/common/translation_helpers.R.

# -------------------------------------------------------------------
# Internal helper: run translate_column() for multiple columns
# -------------------------------------------------------------------

run_column_translations <- function(guides_clean, translate_cfg) {
  # Ensure the translation helper exists.
  if (!exists("translate_column")) {
    stop(
      "translate_column() is not available.\n",
      "Make sure src/common/translation_helpers.R is sourced by _targets.R."
    )
  }
  
  output_dir <- translate_cfg$output_dir %||% "sandbox/translations"
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  service     <- tolower(translate_cfg$service %||% "libretranslate")
  source_lang <- translate_cfg$source_lang %||% "auto"
  target_lang <- translate_cfg$target_lang %||% "en"
  max_cores   <- translate_cfg$max_cores %||% parallel::detectCores()
  batch_size  <- translate_cfg$batch_size %||% 100
  mode        <- translate_cfg$mode %||% "auto"
  
  # Columns to translate:
  #   - if translate_cfg$columns is set, use that list
  #   - otherwise, fall back to the original default columns
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
  
  cols_to_translate <- intersect(columns_cfg, names(guides_clean))
  
  if (!length(cols_to_translate)) {
    warning(
      "No translatable columns found in guides_clean. ",
      "Expected at least one of: ",
      paste(columns_cfg, collapse = ", ")
    )
    return(list())
  }
  
  if (!"document_number" %in% names(guides_clean)) {
    stop(
      "Column 'document_number' not found in guides_clean. ",
      "It is required as an ID for joining translations."
    )
  }
  
  message(
    "Translating columns with service = ", service,
    " and mode = ", mode, ": ",
    paste(cols_to_translate, collapse = ", ")
  )
  
  translation_dfs <- list()
  
  # ------------------------------------------------------------------
  # Reviewer mode: do NOT call the service, only read existing CSVs
  # ------------------------------------------------------------------
  if (identical(mode, "reviewer")) {
    for (col in cols_to_translate) {
      out_file <- file.path(
        output_dir,
        paste0(col, "-", target_lang, "-", service, ".csv")
      )
      
      if (!file.exists(out_file)) {
        stop(
          "run_column_translations(): reviewer mode is enabled but ",
          "translation file does not exist: ", out_file, "\n",
          "Generate it first with translate.mode = 'auto' and then ",
          "switch to 'reviewer' after manual edits."
        )
      }
      
      message(
        "Reviewer mode: loading existing translation file for column '",
        col, "': ", out_file
      )
      
      tr_df <- readr::read_csv(out_file, show_col_types = FALSE)
      
      required_cols <- c("id", "translated_text")
      missing_cols  <- setdiff(required_cols, names(tr_df))
      if (length(missing_cols)) {
        stop(
          "Translation file ", out_file,
          " does not contain required columns: ",
          paste(missing_cols, collapse = ", "), "."
        )
      }
      
      translation_dfs[[col]] <- tr_df
    }
    
    return(translation_dfs)
  }
  
  # ------------------------------------------------------------------
  # Auto mode: call the translation service and (re)create CSVs
  # ------------------------------------------------------------------
  for (col in cols_to_translate) {
    out_file <- file.path(
      output_dir,
      paste0(col, "-", target_lang, "-", service, ".csv")
    )
    
    if (file.exists(out_file)) {
      message("Removing existing translation file: ", out_file)
      file.remove(out_file)
    }
    
    message(
      "Translating column '", col, "' into ", target_lang,
      " using service = ", service
    )
    
    translate_column(
      df          = guides_clean,
      column      = col,
      source_lang = source_lang,
      target_lang = target_lang,
      file_path   = out_file,
      batch_size  = batch_size,
      max_cores   = max_cores,
      id_column   = "document_number",
      service     = service
      # context can be added via translate_cfg later if needed.
    )
    
    if (!file.exists(out_file)) {
      stop(
        "Expected translation file was not created: ", out_file,
        "\nCheck translate_column() implementation."
      )
    }
    
    tr_df <- readr::read_csv(out_file, show_col_types = FALSE)
    
    required_cols <- c("id", "translated_text")
    missing_cols  <- setdiff(required_cols, names(tr_df))
    if (length(missing_cols)) {
      stop(
        "Translation file ", out_file,
        " does not contain required columns: ",
        paste(missing_cols, collapse = ", "), "."
      )
    }
    
    translation_dfs[[col]] <- tr_df
  }
  
  translation_dfs
}


# -------------------------------------------------------------------
# Attach translations to guides_clean
# -------------------------------------------------------------------

attach_translations_to_guides <- function(guides_clean, translation_dfs) {
  df <- guides_clean
  
  if (!length(translation_dfs)) {
    message("No translations to attach, returning guides_clean unchanged.")
    return(df)
  }
  
  for (col in names(translation_dfs)) {
    tr_df   <- translation_dfs[[col]]
    new_col <- paste0(col, "_en")
    
    tr_small <- tr_df %>%
      dplyr::select(
        document_number = id,
        translated_text
      ) %>%
      dplyr::rename(!!new_col := translated_text)
    
    df <- df %>%
      dplyr::left_join(tr_small, by = "document_number")
  }
  
  df
}

# -------------------------------------------------------------------
# Public entry point for the translation phase (all columns at once)
# -------------------------------------------------------------------

translate_guides_table <- function(guides_clean, translate_cfg) {
  if (!isTRUE(translate_cfg$enabled %||% TRUE)) {
    message(
      "Translation disabled in config (translate.enabled = FALSE). ",
      "Returning guides_clean unchanged."
    )
    return(guides_clean)
  }
  
  # Healthcheck is implemented in the common helper
  check_translation_service(translate_cfg)
  
  # Translate the selected fields (one CSV per column)
  translation_dfs <- run_column_translations(guides_clean, translate_cfg)
  
  # Attach *_en columns to the original guides_clean table
  guides_translated <- attach_translations_to_guides(guides_clean, translation_dfs)
  
  guides_translated
}

# -------------------------------------------------------------------
# Helper: translate a single column (used as a dynamic target)
# -------------------------------------------------------------------

translate_guides_column <- function(guides_clean, translate_cfg, column_name) {
  if (!isTRUE(translate_cfg$enabled %||% TRUE)) {
    message(
      "Translation disabled in config (translate.enabled = FALSE). ",
      "Returning guides_clean unchanged."
    )
    return(guides_clean)
  }
  
  # Healthcheck is implemented in the common helper
  check_translation_service(translate_cfg)
  
  # Local copy of the config restricted to a single column
  cfg_single <- translate_cfg
  cfg_single$columns <- column_name
  
  translation_dfs <- run_column_translations(guides_clean, cfg_single)
  guides_translated <- attach_translations_to_guides(guides_clean, translation_dfs)
  
  guides_translated
}
