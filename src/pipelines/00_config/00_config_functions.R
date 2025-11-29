# File: src/pipelines/00_config/00_config_functions.R
# Helpers to load and validate the global pipeline configuration
# from config/pipeline.yml.

load_pipeline_config <- function(path = "config/pipeline.yml") {
  if (!file.exists(path)) {
    stop("Pipeline config file not found: ", path, call. = FALSE)
  }
  
  cfg <- yaml::read_yaml(path)
  
  # Sanity check (non-fatal): warn if main sections are missing
  required_top_level <- c("global", "load", "translate", "sdg_detection")
  missing <- setdiff(required_top_level, names(cfg))
  if (length(missing) > 0) {
    warning(
      "Config is missing top-level section(s): ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }
  
  cfg
}
