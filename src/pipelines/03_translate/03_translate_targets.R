# File: src/pipelines/03_translate/03_translate_targets.R
# Targets for the translation phase.

library(targets)

targets_translate <- list(
  # Extract the translate config block from pipeline_config.
  tar_target(
    translate_config,
    pipeline_config$translate
  ),
  
  # Main translated table:
  # - starts from guides_clean (02_clean output)
  # - uses translate_config to decide service, languages, etc.
  # - returns a data frame with *_en columns added.
  tar_target(
    guides_translated,
    translate_guides_table(
      guides_clean,
      translate_config
    )
  )
)
