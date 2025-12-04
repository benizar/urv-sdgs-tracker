# File: src/pipelines/02_translate/functions/10_paths_io.R
# Purpose:
#   Translation phase IO helpers:
#   - build per-column translation file paths
#   - ensure required files exist depending on mode (auto/reviewer)
#   - read and validate translation CSVs

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
  
  # auto mode: create an empty file if missing so downstream can track it as a file dependency
  if (!file.exists(path)) {
    file.create(path)
  }
  
  path
}

read_translation_file <- function(path) {
  tr_df <- readr::read_csv(path, show_col_types = FALSE)
  
  required_cols <- c("id", "translated_text")
  missing_cols  <- setdiff(required_cols, names(tr_df))
  if (length(missing_cols)) {
    stop(
      "Translation file ", path,
      " does not contain required columns: ",
      paste(missing_cols, collapse = ", "), ".",
      call. = FALSE
    )
  }
  
  tr_df
}
