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
#    - "main"   : run the whole pipeline defined in _targets.R
#                 but only load the main results of each phase
#                 into the global environment (recommended default).
#
#    - "all"    : run the whole pipeline and load **all** targets
#                 into the global environment (can be heavy).
#
#    - "import" : run the import layer, up to a per-course index
#                 (typically targets like `guides_raw` and `guides_index`).
#
#    - "clean"  : run the cleaning layer (e.g. `guides_clean`),
#                 which depends on the import layer.
#
#    - "minimal": lightweight end-to-end: only
#                 `guides_index` and `guides_clean`.
#
#    - "custom" : you specify exactly which targets to run
#                 via the `targets` argument.
#
# 3) Example calls:
#
#    run_pipeline("main")
#    run_pipeline("all")
#    run_pipeline("import")
#    run_pipeline("minimal", remake = TRUE)
#    run_pipeline(mode = "custom", targets = c("guides_index", "guides_clean"))
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
#        run_pipeline("main", use_parallel = FALSE)  # force sequential
#        run_pipeline("main", use_parallel = TRUE)   # force parallel
#
# 5) Remake / clean rebuild:
#
#    - remake = FALSE (default): standard incremental build (only outdated targets).
#    - remake = TRUE:
#        * Full pipeline (mode "all" or "main" with no explicit `targets`):
#            tar_destroy(destroy = "all")  # start from scratch.
#        * Partial run:
#            - If tar_delete() exists (newer targets): delete stored values for
#              selected targets (forces them to rerun next tar_make()).
#            - If tar_delete() does NOT exist (older targets): fallback to
#              tar_destroy(destroy = "meta") (invalidates broadly).
#
# 6) Global environment hygiene:
#
#    - Before loading updated targets into .GlobalEnv, any existing objects whose
#      names coincide with pipeline target names are removed (with a warning).
#      This avoids keeping stale copies of targets in the environment.

library(targets)
library(yaml)
library(future)
library(rlang)

# -------------------------------------------------------------------
# Logging helpers
# -------------------------------------------------------------------

# Centralised path for the pipeline log file.
# This should match the path used inside 99_logging/get_log_path().
get_pipeline_log_path <- function() {
  file.path("sandbox", "logs", "targets_log.csv")
}

# Write (or append) information about the last run to the log CSV.
log_pipeline_run <- function(mode, targets_run, log_path = get_pipeline_log_path()) {
  meta <- targets::tar_meta(
    fields = c(
      name,
      started,
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
    existing <- tryCatch(
      readr::read_csv(log_path, show_col_types = FALSE),
      error = function(e) NULL
    )
    
    if (!is.null(existing) && identical(names(existing), names(meta))) {
      combined <- rbind(existing, meta)
      readr::write_csv(combined, log_path)
      message("log_pipeline_run(): appended metadata to existing log: ", log_path)
    } else {
      warning(
        "log_pipeline_run(): existing log file has an incompatible schema. ",
        "It will be replaced by a new log."
      )
      readr::write_csv(meta, log_path)
      message("log_pipeline_run(): created new log file at: ", log_path)
    }
  }
  
  invisible(NULL)
}

# -------------------------------------------------------------------
# Target name helpers (manifest/store-compatible)
# -------------------------------------------------------------------

# Return target names from the pipeline definition if possible (tar_manifest),
# otherwise fallback to what exists in the store (tar_meta).
get_pipeline_target_names <- function() {
  if (exists("tar_manifest", where = asNamespace("targets"), inherits = FALSE)) {
    man <- tryCatch(
      targets::tar_manifest(fields = name),
      error = function(e) NULL
    )
    if (!is.null(man) && nrow(man)) {
      return(as.character(man$name))
    }
  }
  
  meta <- tryCatch(
    targets::tar_meta(fields = name),
    error = function(e) NULL
  )
  if (!is.null(meta) && nrow(meta)) {
    return(as.character(meta$name))
  }
  
  character(0)
}

# -------------------------------------------------------------------
# Helper: load selected targets into the global environment
# -------------------------------------------------------------------

load_targets_interactive <- function(target_names) {
  if (!length(target_names)) {
    message("No targets requested for loading.")
    return(invisible(NULL))
  }
  
  for (nm in target_names) {
    message("Reading target: ", nm)
    
    obj <- tryCatch(
      targets::tar_read_raw(nm),
      error = function(e) {
        warning("Failed to read target '", nm, "': ", conditionMessage(e))
        NULL
      }
    )
    
    if (!is.null(obj)) {
      assign(nm, obj, envir = .GlobalEnv)
      message("Loaded target into global environment: ", nm)
    }
  }
  
  invisible(NULL)
}

# -------------------------------------------------------------------
# Helper: remove target-named objects from the global environment
# -------------------------------------------------------------------

clean_stale_targets_in_env <- function(remove = TRUE, restrict_to = NULL) {
  target_names <- if (is.null(restrict_to)) get_pipeline_target_names() else restrict_to
  if (!length(target_names)) {
    message("clean_stale_targets_in_env(): no pipeline target names found.")
    return(invisible(character(0)))
  }
  
  env_objs  <- ls(envir = .GlobalEnv, all.names = TRUE)
  to_remove <- intersect(env_objs, target_names)
  
  if (!length(to_remove)) {
    message(
      "clean_stale_targets_in_env(): no objects in the global environment ",
      "match pipeline target names."
    )
    return(invisible(character(0)))
  }
  
  message(
    "The following objects exist in the global environment and match pipeline target names.\n",
    "They will be removed before reloading fresh values:\n  - ",
    paste(sort(to_remove), collapse = "\n  - ")
  )
  
  if (isTRUE(remove)) {
    rm(list = to_remove, envir = .GlobalEnv)
  }
  
  invisible(to_remove)
}

# -------------------------------------------------------------------
# Remake helpers (version-compatible)
# -------------------------------------------------------------------

# Full remake = destroy the whole store.
remake_all_targets <- function() {
  targets::tar_destroy(destroy = "all")
  invisible(TRUE)
}

# Partial remake = prefer tar_delete() if available, otherwise fallback.
remake_selected_targets <- function(target_names) {
  target_names <- unique(as.character(target_names))
  target_names <- target_names[nzchar(target_names)]
  
  if (!length(target_names)) {
    message("remake_selected_targets(): no targets requested.")
    return(invisible(FALSE))
  }
  
  if (exists("tar_delete", where = asNamespace("targets"), inherits = FALSE)) {
    message(
      "remake_selected_targets(): deleting stored values for targets:\n  - ",
      paste(sort(target_names), collapse = "\n  - ")
    )
    # tar_delete() expects a tidyselect expression; any_of(vec) works well here.
    targets::tar_delete(tidyselect::any_of(target_names))
    return(invisible(TRUE))
  }
  
  warning(
    "remake_selected_targets(): tar_delete() is not available in this {targets} version.\n",
    "Falling back to tar_destroy(destroy = 'meta'), which invalidates broadly."
  )
  targets::tar_destroy(destroy = "meta")
  invisible(TRUE)
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

run_pipeline <- function(
    mode = c("all", "main", "import", "clean", "minimal", "custom"),
    targets = NULL,
    load_interactively = TRUE,
    load_targets = NULL,
    log_run = TRUE,
    use_parallel = NULL,
    remake = FALSE
) {
  mode <- match.arg(mode)
  
  # ---------------------------------------------------------------
  # Decide which targets to run
  # ---------------------------------------------------------------
  if (!is.null(targets)) {
    targets_to_run <- as.character(targets)
    message(
      "run_pipeline(): using explicit targets for execution: ",
      paste(targets_to_run, collapse = ", ")
    )
  } else {
    targets_to_run <- switch(
      mode,
      "all"     = NULL,  # full pipeline
      "main"    = NULL,  # full pipeline; only loading differs
      "import"  = c("guides_raw", "guides_index"),
      "clean"   = c("guides_clean"),
      "minimal" = c("guides_index", "guides_clean"),
      "custom"  = stop('run_pipeline(mode = "custom") requires a non-NULL `targets` argument.')
    )
  }
  
  # ---------------------------------------------------------------
  # Optional remake: destroy/delete before running
  # ---------------------------------------------------------------
  if (isTRUE(remake)) {
    if (is.null(targets_to_run)) {
      message(
        "run_pipeline(): remake = TRUE and full pipeline selected (mode = '",
        mode, "'). Destroying ALL targets before running."
      )
      remake_all_targets()
    } else {
      message(
        "run_pipeline(): remake = TRUE. Remaking selected targets before running: ",
        paste(targets_to_run, collapse = ", ")
      )
      remake_selected_targets(targets_to_run)
    }
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
    if (!exists("tar_make_future", where = asNamespace("targets"), inherits = FALSE)) {
      warning(
        "Parallel execution requested, but tar_make_future() is not available.\n",
        "Falling back to tar_make() (sequential)."
      )
      use_parallel_flag <- FALSE
    }
  }
  
  if (use_parallel_flag) {
    message(
      "Using parallel execution via tar_make_future() with ",
      parallel_cfg$workers, " workers."
    )
    future::plan(multisession, workers = parallel_cfg$workers)
    
    if (is.null(targets_to_run)) {
      message('Running full pipeline (mode = "', mode, '").')
      targets::tar_make_future()
    } else {
      message('Running selected targets (mode = "', mode, '") in parallel:')
      print(targets_to_run)
      targets::tar_make_future(names = targets_to_run)
    }
    
    future::plan(sequential)
  } else {
    message("Using sequential execution via tar_make().")
    
    if (is.null(targets_to_run)) {
      message('Running full pipeline (mode = "', mode, '").')
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
  # Optionally load targets into the global environment
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
        "all" = {
          # Prefer manifest (pipeline definition), fallback to store.
          get_pipeline_target_names()
        },
        "main" = c(
          # Main “headline” tables per phase (adjust as needed)
          "guides_raw",
          "guides_index",
          "guides_clean",
          "guides_translated",
          "sdg_config",
          "sdg_input",
          "sdg_hits_long",
          "sdg_hits_wide",
          "guides_sdg"
        ),
        "import"  = c("guides_raw", "guides_index"),
        "clean"   = c("guides_clean"),
        "minimal" = c("guides_index", "guides_clean"),
        "custom"  = targets_to_run,
        character(0)
      )
    }
  }
  
  # Avoid stale objects: warn + remove before loading fresh values.
  # Restrict the cleanup to the set we are about to (re)load to reduce noise.
  clean_stale_targets_in_env(remove = TRUE, restrict_to = load_targets)
  
  load_targets_interactive(load_targets)
  invisible(NULL)
}
