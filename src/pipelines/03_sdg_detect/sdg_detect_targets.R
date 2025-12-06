# File: src/pipelines/03_sdg_detect/sdg_detect_targets.R
# Targets for the SDG detection phase (text2sdg).
#
# Assumptions:
#   - The translation phase produces `guides_translated` with `document_number`
#     and the *_en columns referenced in config/sdg_detect.yml (combine_groups)
#   - Functions used here are defined in:
#       src/pipelines/03_sdg_detect/functions/*.R  (loaded by _targets.R)

library(targets)

targets_sdg <- list(
  
  # sdg_config is provided by 00_config (config/sdg_detect.yml)
  
  # 0a) Enable switch (defaults to TRUE if not present)
  tar_target(
    sdg_enabled,
    is.null(sdg_config$enabled) || isTRUE(sdg_config$enabled)
  ),
  
  # 0b) Query dictionary (for later analysis + compounds source)
  # Priority:
  #   1) sdg_config$dictionary_csv (curated; stable)
  #   2) fallback to text2sdg queries + expansions
  tar_target(
    sdg_query_dictionary,
    build_text2sdg_query_dictionary(
      systems = sdg_config$systems %||% c("Aurora","Elsevier","Auckland","SIRIS","SDGO","SDSN"),
      expansions_csv = sdg_config$queries_expansions_csv %||% "resources/asterisc_expressions_expanded.csv",
      dictionary_csv = sdg_config$dictionary_csv %||% "",
      keep_single_word = TRUE
    )
  ),
  
  # 0c) Compounds: multi-word expressions used to re-compose comma-split features
  # Example: "quality of life" -> fixes "quality, of, life"
  tar_target(
    sdg_compounds,
    {
      if (!nrow(sdg_query_dictionary) || !"expression" %in% names(sdg_query_dictionary)) {
        character(0)
      } else {
        x <- tolower(trimws(as.character(sdg_query_dictionary$expression)))
        x <- x[!is.na(x) & nzchar(x)]
        x <- x[stringr::str_count(x, "\\S+") > 1]
        unique(x)
      }
    }
  ),
  
  # 0d) Precomputed compound substitution rules (fast + deterministic)
  # patterns: "quality of life" -> "quality, of, life"
  # replacements: "quality of life"
  tar_target(
    sdg_compound_rules,
    {
      if (!length(sdg_compounds)) {
        list(patterns = character(0), replacements = character(0))
      } else {
        expr <- unique(tolower(trimws(as.character(sdg_compounds))))
        expr <- expr[!is.na(expr) & nzchar(expr)]
        
        # longest first to avoid partial matches
        ord <- order(stringr::str_count(expr, "\\S+"), nchar(expr), decreasing = TRUE)
        expr <- expr[ord]
        
        list(
          patterns = gsub(" ", ", ", expr, fixed = TRUE),
          replacements = expr
        )
      }
    }
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
            "sdg_detect: missing translated columns in guides_translated: ",
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
  
  # 5b) Expanded features (one row per feature term)
  tar_target(
    sdg_features_long,
    {
      if (!sdg_enabled || !nrow(sdg_hits_raw)) {
        tibble::tibble(
          document_number = character(0),
          section         = character(0),
          system          = character(0),
          sdg             = character(0),
          query_id        = integer(0),
          feature         = character(0),
          feature_raw     = character(0),
          feature_fixed   = character(0),
          in_dictionary   = logical(0)
        )
      } else {
        if (!exists("extract_features_long", mode = "function", inherits = TRUE)) {
          stop(
            "sdg_features_long: extract_features_long() not found.\n",
            "Make sure src/pipelines/03_sdg_detect/functions/15_queries.R is sourced by _targets.R.",
            call. = FALSE
          )
        }
        
        extract_features_long(
          hits = sdg_hits_raw,
          sdg_cfg = sdg_config,
          compound_rules = sdg_compound_rules,
          sdg_query_dictionary = sdg_query_dictionary
        )
      }
    }
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
  
  # 8) Summary per course (per-section lists only)
  tar_target(
    guides_sdg_summary,
    build_guides_sdg_summary(guides_translated, sdg_hits_long, sdg_config, sdg_features_long)
  ),
  
  # 9) Review table per detected SDG (lists only)
  tar_target(
    guides_sdg_review,
    build_guides_sdg_review(guides_translated, sdg_hits_long, sdg_config, sdg_features_long)
  )
)
