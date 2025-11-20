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

# Common helpers (includes translation_helpers.R, etc.)
source_dir_into(
  file.path(project_root, "src", "common"),
  env = helper_env
)

# Import pipeline (01) – resolve_import_dir(), build_guides_raw(), ...
source_dir_into(
  file.path(project_root, "src", "pipelines", "01_import"),
  env = helper_env
)

# Clean pipeline (02) – only if it exists
clean_dir <- file.path(project_root, "src", "pipelines", "02_clean")
if (dir.exists(clean_dir)) {
  source_dir_into(clean_dir, env = helper_env)
}

# Translate pipeline (03) – translate_guides_table(), run_column_translations(), ...
translate_dir <- file.path(project_root, "src", "pipelines", "03_translate")
if (dir.exists(translate_dir)) {
  source_dir_into(translate_dir, env = helper_env)
}

# SDG detection pipeline (04) – build_sdg_input_text(), run_text2sdg_detection(), ...
sdg_dir <- file.path(project_root, "src", "pipelines", "04_detect_sdg")
if (dir.exists(sdg_dir)) {
  source_dir_into(sdg_dir, env = helper_env)
}

# -------------------------------------------------------------------
# 4) Optional sanity checks (can be removed later)
# -------------------------------------------------------------------
if (!exists("resolve_import_dir", envir = helper_env)) {
  warning("Function 'resolve_import_dir' not found after sourcing import pipeline code.")
}

if (!exists("build_sdg_input_text", envir = helper_env)) {
  warning("Function 'build_sdg_input_text' not found after sourcing SDG pipeline code.")
}

if (!exists("translate_guides_table", envir = helper_env)) {
  warning("Function 'translate_guides_table' not found after sourcing translate pipeline code.")
}
