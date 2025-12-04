# File: src/pipelines/03_detect_sdg/03_detect_sdg_targets.R
# Targets for the SDG detection phase (text2sdg).
#
# Assumptions:
#   - The translation phase produces `guides_translated` with `document_number`
#     and the *_en columns referenced in config/translate.yml (combine_groups)
#   - Functions used here are defined in:
#       src/pipelines/03_detect_sdg/03_detect_sdg_functions.R

library(targets)

targets_sdg <- list(

  # sdg_config is provided by 00_config (config/sdg_detection.yml)
  
  # 0a) Enable switch (defaults to TRUE if not present)
  tar_target(
    sdg_enabled,
    is.null(sdg_config$enabled) || isTRUE(sdg_config$enabled)
  ),
  
  # 0b) Query dictionary (for wordclouds / later analysis)
  tar_target(
    sdg_query_dictionary,
    build_text2sdg_query_dictionary(
      systems = sdg_config$systems %||% c("Aurora","Elsevier","Auckland","SIRIS","SDGO","SDSN"),
      expansions_csv = sdg_config$queries_expansions_csv %||% "resources/asterisc_expressions_expanded.csv",
      keep_single_word = isTRUE(sdg_config$keep_single_word_queries %||% FALSE)
    )
  ),
  
  # 1) Build SDG input text:
  #    one row per (document_number, section), with concatenated text.
  tar_target(
    sdg_input,
    {
      if (!sdg_enabled) {
        tibble::tibble(
          document_number = character(0),
          section = character(0),
          text = character(0)
        )
      } else {
        if (!"document_number" %in% names(guides_translated)) {
          stop("`guides_translated` must contain `document_number`.", call. = FALSE)
        }
        
        # Fail fast if the config references *_en columns that are missing.
        needed_cols <- unique(unlist(purrr::map(sdg_config$combine_groups, "columns")))
        missing_cols <- setdiff(needed_cols, names(guides_translated))
        if (length(missing_cols)) {
          stop(
            "sdg_detection: missing translated columns in guides_translated: ",
            paste(missing_cols, collapse = ", "),
            "\n\nAvailable columns are:\n",
            paste(names(guides_translated), collapse = ", "),
            call. = FALSE
          )
        }
        
        build_sdg_input(guides_translated, sdg_config)
      }
    }
  ),
  
  # 2) Helper target: list of sections present in sdg_input.
  tar_target(
    sdg_sections,
    unique(sdg_input$section)
  ),
  
  # 3) Raw hits from text2sdg PER SECTION (dynamic branching).
  tar_target(
    sdg_hits_raw_section,
    {
      if (!sdg_enabled || !nrow(sdg_input)) {
        return(tibble::tibble())
      }
      
      this_section <- sdg_sections
      sdg_input_section <- sdg_input[sdg_input$section == this_section, , drop = FALSE]
      run_text2sdg_detection(sdg_input_section, sdg_config)
    },
    pattern = map(sdg_sections),
    iteration = "list"
  ),
  
  # 4) Combine all per-section results into a single raw hits table.
  tar_target(
    sdg_hits_raw,
    dplyr::bind_rows(sdg_hits_raw_section)
  ),
  
  # 5) Long summary (document_number x section x system x sdg).
  tar_target(
    sdg_hits_long,
    summarise_sdg_hits_long(sdg_hits_raw)
  ),
  
  # 6) Wide summary (one row per document_number, one column per combo).
  tar_target(
    sdg_hits_wide,
    summarise_sdg_hits_wide(sdg_hits_long, sdg_config)
  ),
  
  # 7) Final table with SDG annotations (kept as-is).
  tar_target(
    guides_sdg,
    attach_sdg_to_guides(guides_translated, sdg_hits_wide)
  ),
  
  # 8) NEW: one row per course with summary fields (global + per section)
  tar_target(
    guides_sdg_summary,
    build_guides_sdg_summary(guides_translated, sdg_hits_long, sdg_config)
  ),
  
  # 9) NEW: one row per detected SDG (courses repeat) for manual review workflows
  tar_target(
    guides_sdg_review,
    build_guides_sdg_review(guides_translated, sdg_hits_long, sdg_config)
  )
)
