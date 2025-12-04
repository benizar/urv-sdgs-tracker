# File: src/pipelines/00_config/00_config_functions.R
# Helpers to load per-phase configuration YAML files.

load_yaml_config <- function(path, config_name = NULL, required_fields = NULL) {
  if (!file.exists(path)) {
    stop("Config file not found: ", path, call. = FALSE)
  }
  
  cfg <- yaml::read_yaml(path)
  
  if (is.null(cfg) || !is.list(cfg)) {
    stop("Config file is empty or invalid YAML: ", path, call. = FALSE)
  }
  
  # Non-fatal sanity check (warn only)
  if (!is.null(required_fields)) {
    missing <- setdiff(required_fields, names(cfg))
    if (length(missing) > 0) {
      warning(
        "Config ", if (!is.null(config_name)) paste0("(", config_name, ") ") else "",
        "is missing field(s): ", paste(missing, collapse = ", "),
        "\nFile: ", path,
        call. = FALSE
      )
    }
  }
  
  cfg
}
