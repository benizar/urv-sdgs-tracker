# File: src/pipelines/99_logging/99_logging_targets.R
# Targets for the logging / diagnostics phase.

library(targets)

targets_logging <- list(
  # Raw log table loaded from CSV
  tar_target(
    log_table,
    load_pipeline_log()
  ),
  
  # Aggregated summary by target
  tar_target(
    log_summary_by_target,
    summarise_log_by_target(log_table)
  )
)
