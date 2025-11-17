# File: run_pipeline.R
# Helper to run and inspect the URV SDGs tracker pipeline from RStudio.

library(targets)

# -------------------------------------------------------------------
# Helper: load selected targets into the global environment
# -------------------------------------------------------------------
# This is purely for interactive use in RStudio:
# it reads the values from the {targets} data store and assigns them
# into .GlobalEnv so you can see them in the Environment pane.
load_targets_interactive <- function(target_names) {
  if (!length(target_names)) {
    message("No targets requested for loading.")
    return(invisible(NULL))
  }
  
  for (nm in target_names) {
    message("Reading target: ", nm)
    # tar_read_raw() expects a character string with the target name.
    obj <- targets::tar_read_raw(nm)
    assign(nm, obj, envir = .GlobalEnv)
    message("Loaded target into global environment: ", nm)
  }
  
  invisible(NULL)
}

# -------------------------------------------------------------------
# Main entry point to run the pipeline
# -------------------------------------------------------------------
# Arguments:
#   mode:
#     "all"      : run the whole pipeline defined in _targets.R
#     "scraping" : only run scraping-related targets (00_scraping),
#                  typically "scraping_dir" and its dependencies.
#     "import"   : run the import layer, usually "guides_raw".
#     "minimal"  : run both scraping_dir and guides_raw (basic end-to-end).
#     "custom"   : use the 'targets' argument to specify exactly
#                  which targets to run.
#
#   targets:
#     Character vector of target names to run. If not NULL, these
#     targets are used instead of the defaults for the chosen mode.
#
#   load_interactively:
#     TRUE  : after running, automatically load a small set of key
#             targets into the global environment (see below).
#     FALSE : only run tar_make(), do not load any targets.
#
#   load_targets:
#     Optional character vector of target names to load after running.
#     If NULL, defaults are chosen based on 'mode':
#       - "all"      : c("scraping_dir", "guides_raw")
#       - "scraping" : "scraping_dir"
#       - "import"   : "guides_raw"
#       - "minimal"  : c("scraping_dir", "guides_raw")
#       - "custom"   : whatever was actually run (targets_to_run)
run_pipeline <- function(
    mode = c("all", "scraping", "import", "minimal", "custom"),
    targets = NULL,
    load_interactively = TRUE,
    load_targets = NULL
) {
  mode <- match.arg(mode)
  
  # ---------------------------------------------------------------
  # Decide which targets to run
  # ---------------------------------------------------------------
  if (!is.null(targets)) {
    # Explicit target list overrides mode for execution.
    targets_to_run <- targets
    message(
      "run_pipeline(): using explicit targets for execution: ",
      paste(targets_to_run, collapse = ", ")
    )
  } else {
    # Use defaults based on mode.
    targets_to_run <- switch(
      mode,
      "all"      = NULL,                                # full pipeline
      "scraping" = "scraping_dir",
      "import"   = c("scraping_dir", "guides_raw", "guides_index"),
      "minimal"  = c("scraping_dir", "guides_raw", "guides_index", "guides_index"),
      "custom"   = stop(
        "run_pipeline(mode = \"custom\") requires a non-NULL 'targets' argument."
      )
    )
  }
  
  # ---------------------------------------------------------------
  # Run the pipeline (full or partial)
  # ---------------------------------------------------------------
  if (is.null(targets_to_run)) {
    message("Running full pipeline (mode = \"all\").")
    tar_make()
  } else {
    message("Running selected targets (mode = \"", mode, "\"):")
    print(targets_to_run)
    tar_make(names = targets_to_run)
  }
  
  # ---------------------------------------------------------------
  # Optionally load key targets into the global environment
  # ---------------------------------------------------------------
  if (!isTRUE(load_interactively)) {
    return(invisible(NULL))
  }
  
  if (is.null(load_targets)) {
    load_targets <- if (!is.null(targets)) {
      targets_to_run
    } else {
      switch(
        mode,
        "all"      = c("scraping_dir", "guides_raw", "guides_index", "guides_clean"),
        "scraping" = c("scraping_dir"),
        "import"   = c("guides_raw", "guides_index"),
        "minimal"  = c("scraping_dir", "guides_raw", "guides_index", "guides_clean"),
        "custom"   = targets_to_run,
        character(0)
      )
    }
  }
  
  load_targets_interactive(load_targets)
  invisible(NULL)
}

# -------------------------------------------------------------------
# Example usage (run these from the R console)
# -------------------------------------------------------------------
# source("run_pipeline.R")
#
# # 1) Full pipeline, then load scraping_dir and guides_raw:
# run_pipeline("all")
#
# # 2) Only scraping phase (00_scraping) and load scraping_dir:
# run_pipeline("scraping")
#
# # 3) Only import phase (01_import) and load guides_raw:
# run_pipeline("import")
#
# # 4) Minimal end-to-end: scraping_dir + guides_raw:
# run_pipeline("minimal")
#
# # 5) Custom: run and load only guides_raw:
# run_pipeline(mode = "custom", targets = "guides_raw")
