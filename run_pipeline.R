# File: run_pipeline.R
# Helper to run and inspect the URV SDGs tracker pipeline from RStudio.
#
# -------------------------------------------------------------------
# How to use this helper
# -------------------------------------------------------------------
#
# 1) From a clean RStudio session (inside the Docker container):
#
#    source("run_pipeline.R")
#
# 2) Choose a mode:
#
#    - "all"     : run the whole pipeline defined in _targets.R
#                  (import + clean + translate + sdg, etc.).
#    - "import"  : run the import layer, up to a per-course index
#                  (typically targets like `guides_raw` and `guides_index`).
#    - "clean"   : run the cleaning layer (e.g. `guides_clean`),
#                  which depends on the import layer.
#    - "minimal" : lightweight end-to-end: only
#                  `guides_index` and `guides_clean`.
#    - "custom"  : you specify exactly which targets to run
#                  via the `targets` argument.
#
# 3) Example calls:
#
#    # full pipeline, then load main tables into the Environment pane
#    run_pipeline("all")
#
#    # only import phase (up to guides_index)
#    run_pipeline("import")
#
#    # import + clean, but only load the cleaned table
#    run_pipeline("clean")
#
#    # lightweight end-to-end for quick exploration
#    run_pipeline("minimal")
#
#    # custom: run and load only one target
#    run_pipeline(mode = "custom", targets = "guides_index")
#
# 4) Parallel execution:
#
#    - Global defaults are read from config/pipeline.yml:
#        global:
#          parallel:
#            enabled: true
#            backend: "future"
#            workers: 4
#
#    - You can override this per run:
#        run_pipeline("all", use_parallel = FALSE)  # force sequential
#        run_pipeline("all", use_parallel = TRUE)   # force parallel
#
# -------------------------------------------------------------------
# Notes about scraping
# -------------------------------------------------------------------
#
# Web scraping is now OUTSIDE the {targets} pipeline.
# The pipeline starts at the import layer, which:
#
#   - reads configuration (e.g. URLs, local folders) from a config file
#   - downloads or imports the data as needed
#
# To change the data source, you only edit the config file used by the
# import target(s). You do NOT need a “scraping_dir” target any more.
#
# -------------------------------------------------------------------
# Notes about logging
# -------------------------------------------------------------------
#
# Each time you call run_pipeline(), you can log metadata from the last
# run (target name, runtime, bytes, warnings, etc.) to a CSV file.
# That log can be analysed later by the 99_logging phase
# (targets like log_table, log_summary_by_target, etc.).
#
# Logging is enabled by default (log_run = TRUE).

library(targets)

# We need yaml and future for config + parallel execution.
# (Make sure these packages are installed in your Docker image.)
library(yaml)
library(future)

# -------------------------------------------------------------------
# Logging helpers
# -------------------------------------------------------------------

# Centralised path for the pipeline log file.
# This should match the path used inside 99_logging/get_log_path().
get_pipeline_log_path <- function() {
  file.path("sandbox", "logs", "targets_log.csv")
}

# Write (or append) information about the last run to the log CSV.
#
# Arguments:
#   mode        : value passed to run_pipeline() ("all", "import", ...)
#   targets_run : character vector of targets passed to tar_make()
#                 (NULL if the full pipeline was run)
#   log_path    : path to the CSV log file
log_pipeline_run <- function(mode, targets_run, log_path = get_pipeline_log_path()) {
  meta <- targets::tar_meta(
    fields = c(
      name,
      started,
      finished,
      time,
      seconds,
      bytes,
      error,
      warning,
      pattern,
      iteration
    )
  )
  
  if (!nrow(meta)) {
    message("log_pipeline_run(): no metadata available to log.")
    return(invisible(NULL))
  }
  
  requested <- if (is.null(targets_run)) "all" else paste(targets_run, collapse = ", ")
  
  meta$run_timestamp         <- Sys.time()
  meta$run_mode              <- mode
  meta$run_targets_requested <- requested
  
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  
  if (!file.exists(log_path)) {
    readr::write_csv(meta, log_path)
    message("log_pipeline_run(): created log file at: ", log_path)
  } else {
    readr::write_csv(meta, log_path, append = TRUE)
    message("log_pipeline_run(): appended metadata to log: ", log_path)
  }
  
  invisible(NULL)
}

# -------------------------------------------------------------------
# Helper: load selected targets into the global environment
# -------------------------------------------------------------------
# This is purely for interactive use in RStudio:
# it reads the values from the {targets} store and assigns them
# into .GlobalEnv so you can see them in the Environment pane.

load_targets_interactive <- function(target_names) {
  if (!length(target_names)) {
    message("No targets requested for loading.")
    return(invisible(NULL))
  }
  
  for (nm in target_names) {
    message("Reading target: ", nm)
    obj <- targets::tar_read_raw(nm)
    assign(nm, obj, envir = .GlobalEnv)
    message("Loaded target into global environment: ", nm)
  }
  
  invisible(NULL)
}

# -------------------------------------------------------------------
# Internal: read parallel config from config/pipeline.yml
# -------------------------------------------------------------------
get_parallel_config <- function() {
  cfg_path <- file.path("config", "pipeline.yml")
  if (!file.exists(cfg_path)) {
    return(list(enabled = FALSE, backend = "future", workers = 1L))
  }
  
  cfg <- yaml::read_yaml(cfg_path)
  parallel_cfg <- cfg$global$parallel
  
  if (is.null(parallel_cfg)) {
    return(list(enabled = FALSE, backend = "future", workers = 1L))
  }
  
  list(
    enabled = isTRUE(parallel_cfg$enabled),
    backend = parallel_cfg$backend %||% "future",
    workers = as.integer(parallel_cfg$workers %||% parallel::detectCores())
  )
}

# -------------------------------------------------------------------
# Main entry point to run the pipeline
# -------------------------------------------------------------------
# Arguments:
#   mode:
#     "all"     : run the whole pipeline defined in _targets.R.
#     "import"  : run the import layer (e.g. guides_raw, guides_index).
#     "clean"   : run the cleaning layer (e.g. guides_clean).
#     "minimal" : lightweight end-to-end (index + clean).
#     "custom"  : requires a non-NULL `targets` argument.
#
#   targets:
#     Character vector of target names to run. If not NULL, these
#     targets are used instead of the defaults for the chosen mode.
#
#   load_interactively:
#     TRUE  : after running, automatically load a small set of key
#             targets into the global environment (see below).
#     FALSE : only run tar_make()/tar_make_future(), do not load any.
#
#   load_targets:
#     Optional character vector of target names to load after running.
#     If NULL, defaults are chosen based on 'mode'.
#
#   log_run:
#     TRUE  : after tar_make(), write metadata to the CSV log file.
#     FALSE : do not write any log.
#
#   use_parallel:
#     NULL  : use the setting from config/pipeline.yml (global.parallel.enabled).
#     TRUE  : force parallel execution (tar_make_future()).
#     FALSE : force sequential execution (tar_make()).

run_pipeline <- function(
    mode = c("all", "import", "clean", "minimal", "custom"),
    targets = NULL,
    load_interactively = TRUE,
    load_targets = NULL,
    log_run = TRUE,
    use_parallel = NULL
) {
  mode <- match.arg(mode)
  
  # ---------------------------------------------------------------
  # Decide which targets to run
  # ---------------------------------------------------------------
  if (!is.null(targets)) {
    targets_to_run <- targets
    message(
      "run_pipeline(): using explicit targets for execution: ",
      paste(targets_to_run, collapse = ", ")
    )
  } else {
    targets_to_run <- switch(
      mode,
      "all"     = NULL,  # full pipeline: tar_make()/tar_make_future() decides
      "import"  = c("guides_raw", "guides_index"),
      "clean"   = c("guides_clean"),
      "minimal" = c("guides_index", "guides_clean"),
      "custom"  = stop(
        'run_pipeline(mode = "custom") requires a non-NULL `targets` argument.'
      )
    )
  }
  
  # ---------------------------------------------------------------
  # Decide sequential vs parallel based on config and use_parallel
  # ---------------------------------------------------------------
  parallel_cfg <- get_parallel_config()
  
  if (is.null(use_parallel)) {
    use_parallel_flag <- parallel_cfg$enabled
  } else {
    use_parallel_flag <- isTRUE(use_parallel)
  }
  
  # ---------------------------------------------------------------
  # Run the pipeline (full or partial)
  # ---------------------------------------------------------------
  if (use_parallel_flag) {
    message(
      "Using parallel execution via tar_make_future() with ",
      parallel_cfg$workers, " workers."
    )
    future::plan(multisession, workers = parallel_cfg$workers)
    
    if (is.null(targets_to_run)) {
      message('Running full pipeline (mode = "all").')
      targets::tar_make_future()
    } else {
      message('Running selected targets (mode = "', mode, '") in parallel:')
      print(targets_to_run)
      targets::tar_make_future(names = targets_to_run)
    }
    
    # Optional: reset the future plan to the default (sequential)
    future::plan(sequential)
  } else {
    message("Using sequential execution via tar_make().")
    
    if (is.null(targets_to_run)) {
      message('Running full pipeline (mode = "all").')
      targets::tar_make()
    } else {
      message('Running selected targets (mode = "', mode, '"):')
      print(targets_to_run)
      targets::tar_make(names = targets_to_run)
    }
  }
  
  # ---------------------------------------------------------------
  # Optional logging of the last run
  # ---------------------------------------------------------------
  if (isTRUE(log_run)) {
    log_pipeline_run(mode = mode, targets_run = targets_to_run)
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
        "all"     = c("guides_raw", "guides_index", "guides_clean", "guides_translated", "guides_sdg"),
        "import"  = c("guides_raw", "guides_index"),
        "clean"   = c("guides_clean"),
        "minimal" = c("guides_index", "guides_clean"),
        "custom"  = targets_to_run,
        character(0)
      )
    }
  }
  
  load_targets_interactive(load_targets)
  invisible(NULL)
}
