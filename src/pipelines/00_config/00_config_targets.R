# File: src/pipelines/00_config/00_config_targets.R
# Targets for loading per-phase configuration files.

library(targets)

targets_config <- list(
  # File dependencies (independent invalidation)
  tar_target(global_config_file,    "config/global.yml",        format = "file"),
  tar_target(load_config_file,      "config/load.yml",          format = "file"),
  tar_target(translate_config_file, "config/translate.yml",     format = "file"),
  tar_target(sdg_config_file,       "config/sdg_detect.yml", format = "file"),
  
  # Parsed YAML as R lists
  tar_target(global_config,    load_yaml_config(global_config_file,    config_name = "global")),
  tar_target(load_config,      load_yaml_config(load_config_file,      config_name = "load")),
  tar_target(translate_config, load_yaml_config(translate_config_file, config_name = "translate")),
  tar_target(sdg_config,       load_yaml_config(sdg_config_file,       config_name = "sdg_detect"))
)
