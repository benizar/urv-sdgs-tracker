# File: _targets.R
# Main entry point for the SDGs tracker pipeline

library(targets)

# Set global options for {targets}
tar_option_set(
  packages = c(
    # core workflow
    "tidyverse",
    "readr",
    "jsonlite",
    "openxlsx",
    "yaml",
    "xml2",
    "stringi",
    
    # translation
    "httr",
    "data.table",
    
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
common_dirs <- c("src/common")

common_files <- unlist(lapply(
  common_dirs,
  function(d) {
    if (dir.exists(d)) {
      sort(list.files(d, pattern = "[.][Rr]$", full.names = TRUE))
    } else {
      character(0)
    }
  }
))

if (length(common_files)) tar_source(common_files)

# -------------------------------------------------------------------
# 2) Load project-specific pipeline code (functions + targets)
#    Explicitly list the pipeline folders we want to use.
#    This avoids loading old scripts under src/pipelines/pre, etc.
# -------------------------------------------------------------------
pipeline_dirs <- c(
  "src/pipelines/00_config",
  "src/pipelines/01_load",
  "src/pipelines/02_translate",
  "src/pipelines/03_sdg_detect",
  "src/pipelines/99_logging"
)

pipeline_files <- unlist(lapply(
  pipeline_dirs,
  function(d) {
    if (dir.exists(d)) {
      sort(list.files(d, pattern = "[.][Rr]$", full.names = TRUE, recursive = TRUE))
    } else {
      character(0)
    }
  }
))

if (length(pipeline_files)) tar_source(pipeline_files)

# -------------------------------------------------------------------
# 3) Combine all phase target lists
#    (each one is defined in its own folder under src/pipelines/)
# -------------------------------------------------------------------
list(
  targets_config,       # 00_config/00_config_targets.R
  targets_load,         # 01_load/01_load_targets.R
  targets_translate,    # 02_translate/02_translate_targets.R
  targets_sdg,          # 03_sdg_detect/03_sdg_detect_targets.R
  targets_logging       # 99_logging/99_logging_targets.R
)
