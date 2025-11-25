# File: src/pipelines/04_sdg/04_detect_sdg_targets.R
# Targets for the SDG detection phase (text2sdg).
#
# Assumptions:
#   - There is a target `pipeline_config` defined in 00_config that reads
#     config/pipeline.yml and exposes (among others) `sdg_detection`.
#   - The cleaning phase produces `guides_clean` (not used directly here).
#   - The translation phase produces `guides_translated`, which is a data
#     frame with one row per document_number and *_en text columns.
#   - Functions used here are defined in:
#       src/pipelines/04_sdg/04_detect_sdg_functions.R

library(targets)

targets_sdg <- list(
  # 0) SDG configuration extracted from the global pipeline config.
  #    This reads the `sdg_detection` section of config/pipeline.yml
  #    via the pipeline_config target defined in 00_config.
  tar_target(
    sdg_config,
    pipeline_config$sdg_detection
  ),
  
  # 1) Build SDG input text:
  #    one row per (document_number, section), with concatenated text.
  #    Sections are defined in sdg_config$combine_groups.
  #
  #    Here we always use guides_translated, which already contains
  #    the *_en columns produced by the translation phase (in auto or
  #    reviewer mode).
  tar_target(
    sdg_input,
    build_sdg_input(guides_translated, sdg_config)
  ),
  
  # 2) Helper target: list of sections present in sdg_input.
  #    This is used to create dynamic targets per section.
  tar_target(
    sdg_sections,
    unique(sdg_input$section)
  ),
  
  # 3) Raw hits from text2sdg PER SECTION (dynamic branching).
  #
  # Each branch gets the subset of sdg_input for one section
  # (e.g. "course_info", "competences", "references") and runs
  # run_text2sdg_detection() on it.
  #
  # With a parallel scheduler (tar_make_future()), these branches
  # can run in parallel.
  tar_target(
    sdg_hits_raw_section,
    {
      this_section <- sdg_sections
      sdg_input_section <- sdg_input[sdg_input$section == this_section, , drop = FALSE]
      run_text2sdg_detection(sdg_input_section, sdg_config)
    },
    pattern = map(sdg_sections)
  ),
  
  # 4) Combine all per-section results into a single raw hits table.
  tar_target(
    sdg_hits_raw,
    dplyr::bind_rows(sdg_hits_raw_section)
  ),
  
  # 5) Long summary:
  #    one row per document_number x section x system x sdg,
  #    with the number of hits (n_hits).
  tar_target(
    sdg_hits_long,
    summarise_sdg_hits_long(sdg_hits_raw)
  ),
  
  # 6) Wide summary:
  #    one row per document_number, one column per
  #    (section, system, sdg) combination.
  #
  #    Column names follow:
  #      sdg_<sdg_num>_<system>_<section>
  #    e.g. sdg_7_Aurora_course_info
  tar_target(
    sdg_hits_wide,
    summarise_sdg_hits_wide(sdg_hits_long, sdg_config)
  ),
  
  # 7) Final table with SDG annotations:
  #    guides_translated joined with sdg_hits_wide on document_number.
  tar_target(
    guides_sdg,
    attach_sdg_to_guides(guides_translated, sdg_hits_wide)
  )
)
