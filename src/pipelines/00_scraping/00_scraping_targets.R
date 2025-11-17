# File: src/pipelines/00_scraping/00_scraping_targets.R
# Targets for the scraping configuration and sandbox directory.

library(targets)

targets_scraping <- list(
  # Load scraping config from config/scraping.yml.
  tar_target(
    scraping_config,
    load_scraping_config("config/scraping.yml")
  ),

  # Root URL of the teaching guides website (from config).
  tar_target(
    scraping_root_url,
    scraping_config$root_url
  ),

  # Decide which sandbox scraping directory to use (existing vs new).
  # This target always returns a path like:
  #   sandbox/data/guides_scraping_<run_id>.
  # A message will remind the user that automatic scraping
  # is not implemented yet and must be done with a browser plugin.
  tar_target(
    scraping_dir,
    resolve_scraping_dir(scraping_config)
  )
)

