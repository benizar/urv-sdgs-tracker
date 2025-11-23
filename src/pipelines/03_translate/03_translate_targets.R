# File: src/pipelines/03_translate/03_translate_targets.R
# Targets for the translation phase.

library(targets)

targets_translate <- list(
  # Extract the translate config block from pipeline_config.
  tar_target(
    translate_config,
    pipeline_config$translate
  ),
  
  # Vector of column names to translate, taken from translate_config$columns
  # or falling back to the default set.
  tar_target(
    translate_column_ids,
    {
      cols <- translate_config$columns
      if (is.null(cols) || !length(cols)) {
        cols <- c(
          "course_name_clean",
          "description_clean",
          "contents_clean",
          "competences_learning_results_clean",
          "references_clean"
        )
      }
      cols
    }
  ),
  
  # One dynamic branch per column: each branch returns guides_clean
  # with the corresponding *_en column added.
  tar_target(
    guides_translated_column,
    translate_guides_column(
      guides_clean,
      translate_config,
      column_name = translate_column_ids
    ),
    pattern = map(translate_column_ids),
    iteration = "list"
  ),
  
  # Final merged table: start from guides_clean and join the *_en columns
  # from each dynamic branch.
  tar_target(
    guides_translated,
    {
      Reduce(
        function(df_acc, df_branch) {
          if (!"document_number" %in% names(df_branch)) {
            stop("Branch data frame has no 'document_number' column.")
          }
          
          # Only bring the new translated columns from this branch
          new_cols <- setdiff(names(df_branch), names(df_acc))
          if (!length(new_cols)) {
            return(df_acc)
          }
          
          df_to_join <- df_branch %>%
            dplyr::select(document_number, dplyr::all_of(new_cols))
          
          dplyr::left_join(df_acc, df_to_join, by = "document_number")
        },
        guides_translated_column,
        init = guides_clean
      )
    }
  )
)
