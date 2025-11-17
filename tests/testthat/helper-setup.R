# File: tests/testthat/helper-setup.R
# Common test setup for the URV SDGs tracker project.
#
# This helper is executed automatically by testthat before each test file.
# It loads common helpers and pipeline functions so that tests can call them.

if (!requireNamespace("testthat", quietly = TRUE)) {
  stop("Package 'testthat' is required for running tests.")
}

library(testthat)

if (requireNamespace("targets", quietly = TRUE)) {
  library(targets)
}

# -------------------------------------------------------------------
# 1) Find project root (folder that contains "src")
# -------------------------------------------------------------------
find_project_root <- function() {
  candidates <- c(
    ".",          # running tests from project root
    "..",         # running tests from "tests"
    "../..",      # running tests from "tests/testthat"
    "../../.."    # extra fallback
  )
  
  for (root in candidates) {
    src_path <- file.path(root, "src")
    if (dir.exists(src_path)) {
      return(normalizePath(root, winslash = "/", mustWork = TRUE))
    }
  }
  
  stop(
    "Cannot locate project root (folder that contains 'src').\n",
    "Tried: ", paste(candidates, collapse = ", ")
  )
}

project_root <- find_project_root()

# -------------------------------------------------------------------
# 2) Utility: source all R files from a directory into a given env
# -------------------------------------------------------------------
source_dir_into <- function(path, env) {
  if (!dir.exists(path)) {
    return(invisible(NULL))
  }
  
  files <- list.files(
    path,
    pattern    = "[.][Rr]$",
    full.names = TRUE
  )
  
  for (f in files) {
    # Use sys.source so we can control the environment explicitly
    sys.source(f, envir = env, chdir = TRUE)
  }
  
  invisible(NULL)
}

# We want all functions in the helper environment,
# which is a parent of the test environments.
helper_env <- environment()

# -------------------------------------------------------------------
# 3) Source project code needed in tests
# -------------------------------------------------------------------

# Common helpers
source_dir_into(file.path(project_root, "src", "common"), env = helper_env)

# Scraping pipeline (00) – contains load_scraping_config(), resolve_scraping_dir(), etc.
source_dir_into(
  file.path(project_root, "src", "pipelines", "00_scraping"),
  env = helper_env
)

# Import pipeline (01) – contains resolve_import_dir(), build_guides_raw(), get_guides_raw(), etc.
source_dir_into(
  file.path(project_root, "src", "pipelines", "01_import"),
  env = helper_env
)

# Clean pipeline (02) – only if it exists
clean_dir <- file.path(project_root, "src", "pipelines", "02_clean")
if (dir.exists(clean_dir)) {
  source_dir_into(clean_dir, env = helper_env)
}

# Optional: quick sanity check (you can comment these out later)
if (!exists("resolve_import_dir", envir = helper_env)) {
  warning("Function 'resolve_import_dir' not found after sourcing pipeline code.")
}
if (!exists("load_scraping_config", envir = helper_env)) {
  warning("Function 'load_scraping_config' not found after sourcing pipeline code.")
}
