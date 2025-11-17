# File: src/pipelines/01_import/01_import_targets.R
# Targets for the import phase.

library(targets)

targets_import <- list(
  # High-level raw guides object (list of data frames).
  #
  # For now:
  #   - Reads CSV files from the folder indicated by config/scraping.yml
  #     (sandbox or archive).
  # In the future:
  #   - Can be switched to use an internal scraper without changing
  #     the rest of the pipeline, as long as the return structure
  #     of get_guides_raw(cfg) is preserved.
  tar_target(
    guides_raw,
    get_guides_raw(scraping_config)
  ),
  
  # Main per-course index:
  # one row per course_url with all structural joins and text aggregated
  # by section (description, contents, competences, references, staff, degree).
  # No cleaning or translation is performed here.
  tar_target(
    guides_index,
    build_guides_index_from_raw(guides_raw)
  )
)
