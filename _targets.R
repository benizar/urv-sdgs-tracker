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
    "xml2",
    "stringi",
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
    sort(list.files(common_dir, pattern = "[.][Rr]$", full.names = TRUE)),
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
  "src/pipelines/01_load",
  "src/pipelines/02_translate",
  "src/pipelines/03_detect_sdg",
  "src/pipelines/99_logging"
  # ...
)

pipeline_files <- unlist(lapply(
  pipeline_dirs,
  function(d) {
    if (dir.exists(d)) {
      sort(list.files(d, pattern = "[.][Rr]$", full.names = TRUE))
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
  targets_load,         # 01_load/01_load_targets.R
  targets_translate,    # 02_translate/02_translate_targets.R
  targets_sdg,          # 03_detect_sdg/03_detect_sdg_targets.R
  # targets_validation,   # 04_validation/04_validation_targets.R
  # targets_export,       # 05_export/05_export_targets.R
  # targets_analysis      # 06_analysis/06_analysis_targets.R
  targets_logging       # 99_logging/99_logging_targets.R
)
