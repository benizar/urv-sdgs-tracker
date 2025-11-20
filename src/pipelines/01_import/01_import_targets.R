# File: src/pipelines/01_import/01_import_targets.R
# Targets for the import phase.

library(targets)

targets_import <- list(
  # Import-specific configuration extracted from the global pipeline config.
  tar_target(
    import_config,
    pipeline_config$import
  ),
  
  # High-level raw guides object (list of data frames).
  #
  # Behaviour:
  #   - Uses the `import` section of pipeline_config.
  #   - If the local import directory already exists, it is used as-is.
  #   - If it does not exist, the data are downloaded from the URL
  #     specified in the config (and unzipped if needed).
  tar_target(
    guides_raw,
    get_guides_raw(import_config)
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
