# File: src/pipelines/02_translate/functions/30_attach.R
# Purpose:
#   Attach translation outputs to the main guides table.
#   Provides public entry points for the translate phase.

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
      dplyr::select(document_number = id, translated_text) %>%
      dplyr::rename(!!new_col := translated_text)
    
    df <- df %>%
      dplyr::left_join(tr_small, by = "document_number")
  }
  
  df
}

translate_guides_table <- function(guides_loaded, translate_cfg) {
  if (!isTRUE(translate_cfg$enabled %||% TRUE)) {
    message(
      "Translation disabled in config (translate.enabled = FALSE). ",
      "Returning guides_loaded unchanged."
    )
    return(guides_loaded)
  }
  
  translation_dfs <- run_column_translations(guides_loaded, translate_cfg)
  attach_translations_to_guides(guides_loaded, translation_dfs)
}

translate_guides_column <- function(guides_loaded, translate_cfg, column_name, translation_file = NULL) {
  if (!isTRUE(translate_cfg$enabled %||% TRUE)) {
    message(
      "Translation disabled in config (translate.enabled = FALSE). ",
      "Returning guides_loaded unchanged."
    )
    return(guides_loaded)
  }
  
  cfg_single <- translate_cfg
  cfg_single$columns <- column_name
  
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
  attach_translations_to_guides(guides_loaded, translation_dfs)
}
