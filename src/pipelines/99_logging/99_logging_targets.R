# File: src/pipelines/99_logging/99_logging_targets.R
# Targets to analyse the {targets} run log.

library(targets)

targets_logging <- list(
  # Raw log table (may be empty on the very first run)
  tar_target(
    log_table,
    read_log_safe()
  ),
  
  # Summary by target (empty tibble if there is no log yet)
  tar_target(
    log_summary_by_target,
    summarise_log_by_target(log_table)
  ),
  
  # Summary by run (empty tibble if there is no log yet)
  tar_target(
    log_summary_by_run,
    summarise_log_by_run(log_table)
  )
)
