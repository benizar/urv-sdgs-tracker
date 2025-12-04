# File: src/pipelines/02_translate/functions/20_run.R
# Purpose:
#   Translation execution logic:
#   - run translations per configured columns
#   - supports "auto" (call service) and "reviewer" (read existing CSVs)

run_column_translations <- function(guides_loaded, translate_cfg) {
  if (!exists("translate_column", mode = "function", inherits = TRUE)) {
    stop(
      "translate_column() is not available.\n",
      "Make sure src/common is loaded by _targets.R (tar_source).",
      call. = FALSE
    )
  }
  
  pause_col <- as.numeric(translate_cfg$pause_between_columns_sec %||% 0)
  if (is.na(pause_col) || pause_col < 0) pause_col <- 0
  
  service     <- tolower(translate_cfg$service %||% "libretranslate")
  source_lang <- translate_cfg$source_lang %||% "auto"
  target_lang <- translate_cfg$target_lang %||% "en"
  max_cores   <- translate_cfg$max_cores %||% parallel::detectCores()
  batch_size  <- translate_cfg$batch_size %||% 100
  mode        <- translate_cfg$mode %||% "auto"
  
  overwrite <- isTRUE(translate_cfg$overwrite %||% FALSE)
  
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
      "Available: ", paste(names(guides_loaded), collapse = ", "),
      call. = FALSE
    )
  }
  
  if (!"document_number" %in% names(guides_loaded)) {
    stop(
      "Column 'document_number' not found in guides_loaded. It is required as an ID for joining translations.",
      call. = FALSE
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
      
      out_file <- ensure_translation_file(translate_cfg, col)
      message("Reviewer mode: loading existing translation file for column '", col, "': ", out_file)
      
      translation_dfs[[col]] <- read_translation_file(out_file)
    }
    
    return(translation_dfs)
  }
  
  # ------------------------------------------------------------------
  # Auto mode: call the translation service and (re)create/append CSVs
  # ------------------------------------------------------------------
  for (col in cols_to_translate) {
    if (pause_col > 0) Sys.sleep(pause_col)
    
    out_file <- ensure_translation_file(translate_cfg, col)
    
    if (overwrite && file.exists(out_file)) {
      message("overwrite=TRUE: removing existing translation file: ", out_file)
      file.remove(out_file)
      # ensure translate_column() can re-create schema cleanly
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
    )
    
    if (!file.exists(out_file)) {
      stop(
        "Expected translation file was not created: ", out_file,
        "\nCheck translate_column() implementation.",
        call. = FALSE
      )
    }
    
    translation_dfs[[col]] <- read_translation_file(out_file)
  }
  
  translation_dfs
}
