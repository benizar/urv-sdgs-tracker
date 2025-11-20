# File: _targets.R
# Main entry point for the SDGs tracker pipeline

library(targets)

# Set global options for {targets}
tar_option_set(
  packages = c(
    # core workflow
    "tidyverse",
    "jsonlite",
    "openxlsx",
    "yaml",
    # text / SDG
    "text2sdg",
    "corpustools",
    "tidytext",
    "quanteda",
    "stopwords"
  ),
  format = "rds"
)

# -------------------------------------------------------------------
# 1) Load common, reusable helpers
# -------------------------------------------------------------------
common_dir <- "src/common"

if (dir.exists(common_dir)) {
  purrr::walk(
    list.files(common_dir, pattern = "[.][Rr]$", full.names = TRUE),
    source
  )
}

# -------------------------------------------------------------------
# 2) Load project-specific pipeline code (functions + targets)
#    Explicitly list the pipeline folders we want to use.
#    This avoids loading old scripts under src/pipelines/pre, etc.
# -------------------------------------------------------------------
pipeline_dirs <- c(
  "src/pipelines/00_config",
  "src/pipelines/01_import",
  "src/pipelines/02_clean",
  "src/pipelines/03_translate",
  "src/pipelines/04_detect_sdg",
  "src/pipelines/99_logging"
  # ...
)

pipeline_files <- unlist(lapply(
  pipeline_dirs,
  function(d) {
    if (dir.exists(d)) {
      list.files(d, pattern = "[.][Rr]$", full.names = TRUE)
    } else {
      character(0)
    }
  }
))

lapply(pipeline_files, source)

# -------------------------------------------------------------------
# 3) Combine all phase target lists
#    (each one is defined in its own folder under src/pipelines/)
# -------------------------------------------------------------------
list(
  targets_config,       # 00_config/00_config_targets.R
  targets_import,       # 01_import/01_import_targets.R
  targets_clean,        # 02_clean/02_clean_targets.R
  targets_translate,    # 03_translate/03_translate_targets.R
  targets_sdg,          # 04_detect_sdg/04_detect_sdg_targets.R
  # targets_validation,   # 05_validation/05_validation_targets.R
  # targets_export,       # 06_export/06_export_targets.R
  # targets_analysis      # 07_analysis/07_analysis_targets.R
  targets_logging       # 99_analysis/99_logging_targets.R
)
