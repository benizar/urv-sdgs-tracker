# File: src/pipelines/00_config/00_config_targets.R
# Targets for loading the global pipeline configuration (config/pipeline.yml).

library(targets)

targets_config <- list(
  # File dependency: if config/pipeline.yml changes, this target becomes outdated.
  tar_target(
    pipeline_config_file,
    "config/pipeline.yml",
    format = "file"
  ),
  
  # Parsed YAML configuration as an R list.
  tar_target(
    pipeline_config,
    load_pipeline_config(pipeline_config_file)
  )
)
