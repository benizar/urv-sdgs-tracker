# File: src/pipelines/02_translate/02_translate_targets.R
# Targets for the translation phase.

library(targets)

targets_translate <- list(
  
  # translate_config is provided by 00_config (config/translate.yml)
  
  # Column names to translate must be explicitly provided in config.
  tar_target(
    translate_column_ids,
    {
      cols <- translate_config$columns
      
      if (is.null(cols) || !length(cols)) {
        stop(
          "Missing `translate: columns:` in config/translate.yml.\n\n",
          "Please specify the columns to translate, e.g.:\n",
          "translate:\n",
          "  columns:\n",
          "    - course_description\n",
          "    - course_contents\n",
          "    - course_competences_and_results\n",
          "    - course_references\n",
          call. = FALSE
        )
      }
      
      if (!"document_number" %in% names(guides_loaded)) {
        stop("`guides_loaded` must contain `document_number` before translation.", call. = FALSE)
      }
      
      cols <- unique(as.character(cols))
      
      missing_in_data <- setdiff(cols, names(guides_loaded))
      if (length(missing_in_data) > 0) {
        stop(
          "The following columns listed in `translate: columns:` do not exist in `guides_loaded`: ",
          paste(missing_in_data, collapse = ", "),
          "\n\nAvailable columns are:\n",
          paste(names(guides_loaded), collapse = ", "),
          call. = FALSE
        )
      }
      
      cols
    }
  ),
  
  # One file target per column. Declared as `format = "file"` so edits to the CSV
  # (reviewer mode) automatically invalidate downstream targets.
  tar_target(
    translation_file,
    ensure_translation_file(translate_config, column_name = translate_column_ids),
    pattern = map(translate_column_ids),
    format = "file"
  ),
  
  # One dynamic branch per column: adds the corresponding *_en column.
  # We map over `translation_file` too, so the branch depends on the on-disk CSV.
  tar_target(
    guides_translated_column,
    translate_guides_column(
      guides_loaded,
      translate_config,
      column_name = translate_column_ids,
      translation_file = translation_file
    ),
    pattern = map(translate_column_ids, translation_file),
    iteration = "list"
  ),
  
  # Merge the translated columns back into a single table.
  tar_target(
    guides_translated,
    {
      Reduce(
        function(df_acc, df_branch) {
          if (!"document_number" %in% names(df_branch)) {
            stop("Branch data frame has no 'document_number' column.", call. = FALSE)
          }
          
          new_cols <- setdiff(names(df_branch), names(df_acc))
          if (!length(new_cols)) return(df_acc)
          
          df_to_join <- df_branch |>
            dplyr::select(document_number, dplyr::all_of(new_cols))
          
          dplyr::left_join(df_acc, df_to_join, by = "document_number")
        },
        guides_translated_column,
        init = guides_loaded
      )
    }
  )
)
