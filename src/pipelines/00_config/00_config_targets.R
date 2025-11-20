# File: src/pipelines/00_config/00_config_targets.R
# Targets for global pipeline configuration.

library(targets)

targets_config <- list(
  tar_target(
    pipeline_config,
    load_pipeline_config()
  )
)
