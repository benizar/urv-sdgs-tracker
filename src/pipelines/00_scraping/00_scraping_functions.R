# File: src/pipelines/00_scraping/00_scraping_functions.R
# Scraping helper functions for the URV SDGs tracker.
#
# NOTE:
#   Automatic scraping is NOT implemented yet in this project.
#   For now, please use a browser plugin (for example:
#   "Web Scraper" for Firefox/Chrome) to download the guides data
#   as CSV files and place them under:
#     sandbox/data/guides_scraping_<run_id>.

load_scraping_config <- function(path = file.path("config", "scraping.yml")) {
  if (!file.exists(path)) {
    stop("Scraping config file not found: ", path)
  }
  yaml::read_yaml(path)
}

ensure_scraping_dir <- function(run_id, base_dir) {
  dir_name <- file.path(base_dir, paste0("guides_scraping_", run_id))

  if (!dir.exists(dir_name)) {
    stop(
      "Scraping directory does not exist: ", dir_name,
      "\nPlease create it manually and place the scraped CSV files there, ",
      "or change config/scraping.yml to mode: \"new\"."
    )
  }

  message("Using existing scraping directory: ", dir_name)
  dir_name
}

create_scraping_dir <- function(
  root_url,
  base_dir,
  run_id  = format(Sys.Date(), "%Y%m%d")
) {
  dir_name <- file.path(base_dir, paste0("guides_scraping_", run_id))

  if (!dir.exists(dir_name)) {
    dir.create(dir_name, recursive = TRUE)
    message("Created scraping directory: ", dir_name)
  } else {
    message("Using existing scraping directory: ", dir_name)
  }

  message("Scraping root URL (placeholder, no crawling yet): ", root_url)

  # Here is where automatic scraping will be plugged in in the future.
  # For now, CSV files must be created with an external browser plugin.

  dir_name
}

resolve_scraping_dir <- function(cfg) {
  mode    <- cfg$mode %||% "existing"
  run_id  <- cfg$run_id
  base    <- cfg$sandbox_base_dir %||% "sandbox/data"
  root    <- cfg$root_url
  input   <- cfg$input_location %||% "archive"
  
  if (is.null(run_id)) {
    stop("config/scraping.yml: 'run_id' is not set.")
  }
  
  dir <- file.path(base, paste0("guides_scraping_", run_id))
  
  if (mode == "existing") {
    if (!dir.exists(dir)) {
      if (input == "archive") {
        message(
          "Sandbox scraping directory does not exist: ", dir,
          "\nThis is fine because input_location == 'archive'. ",
          "The pipeline will use only archived scrapings under data/scrapings_archive/."
        )
      } else {
        stop(
          "Scraping directory does not exist: ", dir,
          "\nPlease create it manually and place the scraped CSV files there, ",
          "or change config/scraping.yml to mode: \"new\"."
        )
      }
    } else {
      message("Using existing sandbox scraping directory: ", dir)
    }
    
  } else if (mode == "new") {
    dir <- create_scraping_dir(
      root_url = root,
      base_dir = base,
      run_id   = run_id
    )
  } else {
    stop("Unknown scraping mode in config: ", mode)
  }
  
  message(
    "Note: automatic scraping is not implemented yet. ",
    "Use a browser plugin such as 'Web Scraper' (Firefox/Chrome) ",
    "to export CSV files into the sandbox or archive folders."
  )
  
  dir
}
