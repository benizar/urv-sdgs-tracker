# File: src/pipelines/02_translate/02_translate_targets.R
# Targets for the translation phase.

library(targets)

targets_translate <- list(
  
  # translate_config is provided by 00_config (config/translate.yml)
  
  tar_target(
    translate_cfg_effective,
    {
      cfg <- translate_config
      
      svc <- tolower(cfg$service %||% "libretranslate")
      
      if (is.null(cfg$healthcheck_path) || !nzchar(cfg$healthcheck_path)) {
        cfg$healthcheck_path <- if (svc == "apertium") "/listPairs" else "/languages"
      }
      
      cfg$check_service <- cfg$check_service %||% TRUE
      
      cfg
    }
  ),
  
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
  
  # Decide whether we will actually call the translation API in this run
  tar_target(
    needs_translation_api,
    {
      cfg <- translate_cfg_effective
      
      enabled <- isTRUE(cfg$enabled %||% TRUE)
      mode    <- cfg$mode %||% "auto"
      
      # Never call API if disabled or reviewer mode
      if (!enabled || identical(mode, "reviewer")) {
        return(FALSE)
      }
      
      cols <- translate_column_ids
      if (!length(cols)) return(FALSE)
      
      # Do we have any non-empty text in any selected column?
      any(vapply(cols, function(col) {
        v <- guides_loaded[[col]]
        any(!is.na(v) & nzchar(trimws(as.character(v))))
      }, logical(1)))
    }
  ),
  
  # Healthcheck as a target (runs ONLY if we expect API calls)
  tar_target(
    translation_service_healthcheck,
    {
      if (isTRUE(needs_translation_api)) {
        check_translation_service(translate_cfg_effective)
      } else {
        message("Skipping translation service healthcheck (no API work expected).")
      }
      TRUE
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
    {
      translation_service_healthcheck  # dependency
      
      translate_guides_column(
        guides_loaded,
        translate_config,
        column_name = translate_column_ids,
        translation_file = translation_file
      )
    },
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
