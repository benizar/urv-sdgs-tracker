# File: tests/testthat/helper-setup.R
# Common test setup for the URV SDGs tracker project.

if (!requireNamespace("testthat", quietly = TRUE)) {
  stop("Package 'testthat' is required for running tests.")
}

library(testthat)

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(rlang)
})

find_project_root <- function(start = getwd()) {
  p <- normalizePath(start, winslash = "/", mustWork = FALSE)
  
  repeat {
    if (file.exists(file.path(p, "_targets.R"))) return(p)
    parent <- dirname(p)
    if (identical(parent, p)) return(NULL)
    p <- parent
  }
}

source_dir_files <- function(dir, pattern) {
  if (!dir.exists(dir)) {
    message("helper-setup: directory not found, skipping: ", dir)
    return(invisible(NULL))
  }
  
  files <- list.files(dir, pattern = pattern, full.names = TRUE)
  if (!length(files)) {
    message("helper-setup: no files matched pattern in: ", dir)
    return(invisible(NULL))
  }
  
  for (f in files) {
    source(f, local = .GlobalEnv)
  }
  
  invisible(NULL)
}

root <- find_project_root()
if (is.null(root)) {
  stop(
    "Could not find project root (no _targets.R found). ",
    "Run tests from within the project directory."
  )
}

# Source common helpers
source_dir_files(file.path(root, "src/common"), pattern = "[.][Rr]$")

# Source pipeline functions only (avoid targets definitions in tests)
source_dir_files(file.path(root, "src/pipelines/01_load"), pattern = "_functions[.][Rr]$")
source_dir_files(file.path(root, "src/pipelines/02_translate"), pattern = "_functions[.][Rr]$")

# SDG functions: support both possible folder names (in case you moved it)
source_dir_files(file.path(root, "src/pipelines/03_detect_sdg"), pattern = "_functions[.][Rr]$")
source_dir_files(file.path(root, "src/pipelines/03_sdg"),        pattern = "_functions[.][Rr]$")

# Optional quick sanity check (comment out if you prefer silence)
needed <- c("resolve_load_dir", "translation_file_path", "ensure_translation_file", "build_sdg_input")
missing <- needed[!vapply(needed, exists, logical(1), inherits = TRUE)]
if (length(missing)) {
  message("helper-setup: functions still missing: ", paste(missing, collapse = ", "))
}
