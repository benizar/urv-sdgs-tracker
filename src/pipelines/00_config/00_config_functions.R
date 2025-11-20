# File: src/pipelines/00_config/00_config_functions.R
# Helpers to load and validate the global pipeline configuration
# from config/pipeline.yml.

load_pipeline_config <- function(path = file.path("config", "pipeline.yml")) {
  if (!file.exists(path)) {
    stop(
      "load_pipeline_config(): config file not found at: ", path,
      "\nCreate config/pipeline.yml or adjust the path."
    )
  }
  
  cfg <- yaml::read_yaml(path)
  
  # Basic sanity checks (can be extended later).
  required_top_level <- c("import", "translate", "sdg_detection")
  missing <- setdiff(required_top_level, names(cfg))
  if (length(missing)) {
    warning(
      "load_pipeline_config(): missing top-level sections in pipeline.yml: ",
      paste(missing, collapse = ", ")
    )
  }
  
  cfg
}
