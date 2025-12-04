# File: src/pipelines/02_translate/02_translate_functions.R
# Translation helpers for the URV SDGs tracker pipeline.
#
# Responsibilities:
#   - call the generic translate_column() helper from src/common
#     to produce per-column CSV files
#   - read those CSVs back and attach *_en columns to guides_loaded
#
# NOTE:
# - rtrim_slash() is defined in src/common/translation_helpers.R and loaded from _targets.R.
# - translate_column() is also defined in src/common/translation_helpers.R.

# -------------------------------------------------------------------
# Internal helper: run translate_column() for multiple columns
# -------------------------------------------------------------------

# Helper: build the expected translation CSV path for a given column.
translation_file_path <- function(translate_cfg, column_name) {
  output_dir  <- translate_cfg$output_dir %||% "sandbox/translations"
  service     <- tolower(translate_cfg$service %||% "libretranslate")
  target_lang <- translate_cfg$target_lang %||% "en"
  
  file.path(output_dir, paste0(column_name, "-", target_lang, "-", service, ".csv"))
}

ensure_translation_file <- function(translate_cfg, column_name) {
  path <- translation_file_path(translate_cfg, column_name)
  mode <- translate_cfg$mode %||% "auto"
  
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  if (identical(mode, "reviewer")) {
    if (!file.exists(path)) {
      stop(
        "Reviewer mode is enabled but translation file does not exist: ", path, "\n",
        "Generate it first with translate.mode = 'auto' and then switch to 'reviewer'.",
        call. = FALSE
      )
    }
    return(path)
  }
  
  # auto mode: ensure the file exists so targets can track it as a file dependency
  if (!file.exists(path)) {
    file.create(path)
  }
  
  path
}

run_column_translations <- function(guides_loaded, translate_cfg) {
  # Ensure the translation helper exists.
  if (!exists("translate_column")) {
    stop(
      "translate_column() is not available.\n",
      "Make sure src/common/translation_helpers.R is sourced by _targets.R."
    )
  }
  
  pause_col <- as.numeric(translate_cfg$pause_between_columns_sec %||% 0)
  if (is.na(pause_col) || pause_col < 0) pause_col <- 0
  
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
  #   - translate_cfg$columns must be explicitly provided in config
  columns_cfg <- translate_cfg$columns
  
  if (is.null(columns_cfg) || !length(columns_cfg)) {
    stop(
      "run_column_translations(): missing `columns:` in config/translate.yml.\n\n",
      "Please specify the columns to translate, e.g.:\n",
      "columns:\n",
      "  - course_description\n",
      "  - course_contents\n",
      "  - course_competences_and_results\n",
      "  - course_references\n",
      call. = FALSE
    )
  }
  
  cols_to_translate <- intersect(columns_cfg, names(guides_loaded))
  
  if (!length(cols_to_translate)) {
    stop(
      "run_column_translations(): none of the columns listed in `translate: columns:` exist in guides_loaded.\n",
      "Requested: ", paste(columns_cfg, collapse = ", "), "\n",
      "Available: ", paste(names(guides_loaded), collapse = ", ")
    )
  }
  
  if (!"document_number" %in% names(guides_loaded)) {
    stop(
      "Column 'document_number' not found in guides_loaded. ",
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
      if (pause_col > 0) Sys.sleep(pause_col)
      
      out_file <- translation_file_path(translate_cfg, col)
      
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
    if (pause_col > 0) Sys.sleep(pause_col)
    
    out_file <- translation_file_path(translate_cfg, col)
    
    if (file.exists(out_file)) {
      message("Removing existing translation file: ", out_file)
      file.remove(out_file)
    }
    
    message(
      "Translating column '", col, "' into ", target_lang,
      " using service = ", service
    )
    
    translate_column(
      df          = guides_loaded,
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
# Attach translations to guides_loaded
# -------------------------------------------------------------------

attach_translations_to_guides <- function(guides_loaded, translation_dfs) {
  df <- guides_loaded
  
  if (!length(translation_dfs)) {
    message("No translations to attach, returning guides_loaded unchanged.")
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

translate_guides_table <- function(guides_loaded, translate_cfg) {
  if (!isTRUE(translate_cfg$enabled %||% TRUE)) {
    message(
      "Translation disabled in config (translate.enabled = FALSE). ",
      "Returning guides_loaded unchanged."
    )
    return(guides_loaded)
  }
  
  # Translate the selected fields (one CSV per column)
  translation_dfs <- run_column_translations(guides_loaded, translate_cfg)
  
  # Attach *_en columns to the original guides_loaded table
  guides_translated <- attach_translations_to_guides(guides_loaded, translation_dfs)
  
  guides_translated
}

# -------------------------------------------------------------------
# Helper: translate a single column (used as a dynamic target)
# -------------------------------------------------------------------

translate_guides_column <- function(guides_loaded, translate_cfg, column_name, translation_file = NULL) {
  if (!isTRUE(translate_cfg$enabled %||% TRUE)) {
    message(
      "Translation disabled in config (translate.enabled = FALSE). ",
      "Returning guides_loaded unchanged."
    )
    return(guides_loaded)
  }
  
  # Local copy of the config restricted to a single column
  cfg_single <- translate_cfg
  cfg_single$columns <- column_name
  
  # If a file path is provided by targets, force output_dir to match it
  # and verify naming consistency.
  if (!is.null(translation_file)) {
    cfg_single$output_dir <- dirname(translation_file)
    
    expected <- translation_file_path(cfg_single, column_name)
    if (!identical(
      normalizePath(expected, winslash = "/", mustWork = FALSE),
      normalizePath(translation_file, winslash = "/", mustWork = FALSE)
    )) {
      stop(
        "translation_file mismatch for column '", column_name, "'.\n",
        "Expected: ", expected, "\n",
        "Got:      ", translation_file,
        call. = FALSE
      )
    }
  }
  
  translation_dfs <- run_column_translations(guides_loaded, cfg_single)
  guides_translated <- attach_translations_to_guides(guides_loaded, translation_dfs)
  
  guides_translated
}
