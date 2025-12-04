# File: run_pipeline.R
# Helper to run and inspect the URV SDGs tracker pipeline from RStudio.
#
# Notes:
# - Parallel execution is controlled by config/global under:
#     parallel:
#       enabled: true/false
#       workers: 4
# - You can temporarily override it per run with `use_parallel = TRUE/FALSE`.
# - If parallel execution fails for any reason, the runner falls back to sequential.
#
# - Store cleaning (invalidate/delete) is controlled by `clean`:
#     clean = "all"         -> equivalent to tar_destroy(destroy = "all")
#     clean = "some_target" -> invalidates/deletes that target in the store so it rebuilds,
#                              and downstream targets rebuild automatically as needed.
#
# Usage examples:
#
#   # 1) Run everything and load everything into the global environment
#   run_pipeline(mode = "all")
#
#   # 2) Run the full pipeline but load only the main objects
#   run_pipeline(mode = "main")
#
#   # 3) Run up to the end of the load phase (dependencies run automatically)
#   run_pipeline(mode = "load")
#
#   # 4) Run up to the end of the translate phase
#   run_pipeline(mode = "translate")
#
#   # 5) Run up to the end of the SDG detection phase
#   run_pipeline(mode = "sdg")
#
#   # 6) Custom run: choose exactly which targets to build
#   run_pipeline(mode = "custom", targets = c("guides_loaded", "guides_translated"))
#
#   # 7) Clean the store (like "make clean all"), then run again
#   run_pipeline(mode = "load", clean = "all")
#
#   # 8) Force rebuild from a target (invalidate from there), then run SDG
#   run_pipeline(mode = "sdg", clean = "guides_translated")
#
#   # 9) Run only a final target, but load only that target into the global env
#   run_pipeline(mode = "sdg", load_targets = "guides_sdg")

library(targets)
library(yaml)
library(future)
library(rlang)

# -------------------------------------------------------------------
# Logging helpers
# -------------------------------------------------------------------
get_targets_log_path <- function() {
  file.path("sandbox", "logs", "targets_log.csv")
}

log_pipeline_run <- function(mode, targets_run, log_path = get_targets_log_path()) {
  meta <- tryCatch(targets::tar_meta(), error = function(e) NULL)
  
  if (is.null(meta) || !nrow(meta)) {
    message("log_pipeline_run(): no metadata available to log.")
    return(invisible(NULL))
  }
  
  # Keep a stable schema across runs and {targets} versions.
  wanted <- c(
    "name",
    "started",
    "ended",
    "seconds",
    "bytes",
    "error",
    "warning",
    "warnings",
    "pattern",
    "iteration"
  )
  
  for (nm in wanted) {
    if (!nm %in% names(meta)) meta[[nm]] <- NA
  }
  
  meta <- meta[, wanted, drop = FALSE]
  
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

get_pipeline_target_names <- function() {
  if (exists("tar_manifest", where = asNamespace("targets"), inherits = FALSE)) {
    man <- tryCatch(targets::tar_manifest(fields = name), error = function(e) NULL)
    if (!is.null(man) && nrow(man)) return(as.character(man$name))
  }
  
  meta <- tryCatch(targets::tar_meta(), error = function(e) NULL)
  if (!is.null(meta) && nrow(meta) && "name" %in% names(meta)) return(as.character(meta$name))
  
  character(0)
}

# -------------------------------------------------------------------
# Load/run presets per phase
# -------------------------------------------------------------------

get_run_presets <- function() {
  list(
    # "all" means: build everything (targets_to_run = NULL)
    all = NULL,
    
    # "main" means: build everything (still NULL), but load only key objects by default
    main = NULL,
    
    # run only the final target of each phase
    load      = c("guides_loaded"),
    translate = c("guides_translated"),
    
    # SDG phase: prefer reviewer-friendly outputs
    sdg       = c("guides_sdg_summary", "guides_sdg_review")
  )
}

get_load_presets <- function() {
  list(
    # load everything produced by the pipeline
    all = get_pipeline_target_names(),
    
    # load only main inspection objects (fast + practical)
    main = c(
      "load_config",
      "translate_config",
      "sdg_config",
      
      "guides_loaded",
      "guides_translated",
      
      "sdg_input",
      "sdg_hits_raw",
      "sdg_hits_long",
      "sdg_features_long",
      "sdg_hits_wide",
      
      "guides_sdg_summary",
      "guides_sdg_review"
    ),
    
    # per-phase defaults
    load      = c("guides_loaded"),
    translate = c("guides_translated"),
    sdg       = c("guides_sdg_summary", "guides_sdg_review")
  )
}

# -------------------------------------------------------------------
# Helper: load selected targets into the global environment
# -------------------------------------------------------------------

load_targets_interactive <- function(target_names) {
  target_names <- unique(as.character(target_names))
  target_names <- target_names[nzchar(target_names)]
  
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
  
  if (isTRUE(remove)) rm(list = to_remove, envir = .GlobalEnv)
  invisible(to_remove)
}

# -------------------------------------------------------------------
# Clean helpers (store invalidation)
# -------------------------------------------------------------------

# Disable interactive "Delete .targets?" prompt only for the scope of expr.
with_tar_ask_disabled <- function(expr) {
  old <- Sys.getenv("TAR_ASK", unset = NA_character_)
  Sys.setenv(TAR_ASK = "false")
  on.exit({
    if (is.na(old)) Sys.unsetenv("TAR_ASK") else Sys.setenv(TAR_ASK = old)
  }, add = TRUE)
  force(expr)
}

clean_all_targets <- function() {
  with_tar_ask_disabled(targets::tar_destroy(destroy = "all"))
  invisible(TRUE)
}

clean_from_targets <- function(target_names) {
  target_names <- unique(as.character(target_names))
  target_names <- target_names[nzchar(target_names)]
  
  if (!length(target_names)) {
    message("clean_from_targets(): no targets requested.")
    return(invisible(FALSE))
  }
  
  # Prefer tar_invalidate() when available (marks targets as outdated; keeps stored data).
  if (exists("tar_invalidate", where = asNamespace("targets"), inherits = FALSE)) {
    message(
      "clean_from_targets(): invalidating targets (downstream will rebuild automatically):\n  - ",
      paste(sort(target_names), collapse = "\n  - ")
    )
    
    rlang::inject(
      targets::tar_invalidate(tidyselect::any_of(!!target_names))
    )
    
    return(invisible(TRUE))
  }
  
  # Fallback to tar_delete() (removes stored values; downstream will rebuild automatically).
  if (exists("tar_delete", where = asNamespace("targets"), inherits = FALSE)) {
    message(
      "clean_from_targets(): deleting stored values for targets (downstream will rebuild automatically):\n  - ",
      paste(sort(target_names), collapse = "\n  - ")
    )
    
    rlang::inject(
      targets::tar_delete(tidyselect::any_of(!!target_names))
    )
    
    return(invisible(TRUE))
  }
  
  warning(
    "clean_from_targets(): neither tar_invalidate() nor tar_delete() is available in this {targets} version.\n",
    "Falling back to tar_destroy(destroy = 'meta'), which invalidates broadly."
  )
  with_tar_ask_disabled(targets::tar_destroy(destroy = "meta"))
  invisible(TRUE)
}

# -------------------------------------------------------------------
# Internal: read parallel config from config/global.yml
# -------------------------------------------------------------------

get_parallel_config <- function() {
  cfg_path <- file.path("config", "global.yml")
  if (!file.exists(cfg_path)) return(list(enabled = FALSE, backend = "future", workers = 1L))
  
  cfg <- yaml::read_yaml(cfg_path)
  parallel_cfg <- cfg$parallel
  
  if (is.null(parallel_cfg)) return(list(enabled = FALSE, backend = "future", workers = 1L))
  
  list(
    enabled = isTRUE(parallel_cfg$enabled),
    backend = parallel_cfg$backend %||% "future",
    workers = as.integer(parallel_cfg$workers %||% parallel::detectCores())
  )
}

# -------------------------------------------------------------------
# Internal: print a compact error summary if something failed
# -------------------------------------------------------------------

print_errored_targets_summary <- function() {
  meta <- tryCatch(targets::tar_meta(), error = function(e) NULL)
  
  if (is.null(meta) || !is.data.frame(meta) || nrow(meta) == 0) {
    return(invisible(NULL))
  }
  
  if (!all(c("name", "error") %in% names(meta))) {
    return(invisible(NULL))
  }
  
  err <- dplyr::filter(meta[, c("name", "error"), drop = FALSE], !is.na(error) & nzchar(error))
  if (nrow(err) == 0) return(invisible(NULL))
  
  message("Some targets errored:")
  print(err, row.names = FALSE)
  invisible(NULL)
}

# -------------------------------------------------------------------
# Main entry point to run the pipeline
# -------------------------------------------------------------------

run_pipeline <- function(
    mode = c("all", "main", "load", "translate", "sdg", "custom"),
    targets = NULL,
    load_interactively = TRUE,
    load_targets = NULL,
    log_run = TRUE,
    use_parallel = NULL,
    clean = NULL,
    reporter = c("progress", "verbose")
) {
  mode <- match.arg(mode)
  reporter <- match.arg(reporter)
  
  run_presets  <- get_run_presets()
  load_presets <- get_load_presets()
  
  # ---------------------------------------------------------------
  # Decide which targets to run
  # ---------------------------------------------------------------
  if (!is.null(targets)) {
    targets_to_run <- unique(as.character(targets))
    targets_to_run <- targets_to_run[nzchar(targets_to_run)]
    message(
      "run_pipeline(): using explicit targets for execution: ",
      paste(targets_to_run, collapse = ", ")
    )
  } else {
    targets_to_run <- switch(
      mode,
      "all"       = run_presets$all,
      "main"      = run_presets$main,
      "load"      = run_presets$load,
      "translate" = run_presets$translate,
      "sdg"       = run_presets$sdg,
      "custom"    = stop('run_pipeline(mode = "custom") requires a non-NULL `targets` argument.')
    )
  }
  
  # ---------------------------------------------------------------
  # Optional clean (store invalidation)
  # ---------------------------------------------------------------
  if (!is.null(clean)) {
    if (is.character(clean) && length(clean) == 1L && identical(clean, "all")) {
      message("run_pipeline(): clean = 'all'. Destroying ALL targets.")
      clean_all_targets()
    } else {
      clean_targets <- unique(as.character(clean))
      clean_targets <- clean_targets[nzchar(clean_targets)]
      message(
        "run_pipeline(): clean requested. Invalidating from target(s): ",
        paste(clean_targets, collapse = ", ")
      )
      clean_from_targets(clean_targets)
    }
  }
  
  # ---------------------------------------------------------------
  # Decide sequential vs parallel (config-driven, with simple override)
  # ---------------------------------------------------------------
  parallel_cfg <- get_parallel_config()
  
  if (is.null(use_parallel)) {
    use_parallel_flag <- isTRUE(parallel_cfg$enabled)
  } else {
    use_parallel_flag <- isTRUE(use_parallel)
  }
  
  can_parallel <- use_parallel_flag &&
    exists("tar_make_future", where = asNamespace("targets"), inherits = FALSE)
  
  if (use_parallel_flag && !can_parallel) {
    warning(
      "Parallel execution requested but tar_make_future() is not available.\n",
      "Falling back to tar_make() (sequential)."
    )
  }
  
  # ---------------------------------------------------------------
  # Run the pipeline (parallel with fallback to sequential)
  # ---------------------------------------------------------------
  if (can_parallel) {
    message("Using parallel execution via tar_make_future() with ", parallel_cfg$workers, " workers.")
    
    backend <- tolower(parallel_cfg$backend %||% "multisession")
    backend_fun <- switch(
      backend,
      "future"       = future::multisession,
      "multisession" = future::multisession,
      "multicore"    = future::multicore,
      future::multisession
    )
    
    future::plan(backend_fun, workers = parallel_cfg$workers)
    on.exit(future::plan(sequential), add = TRUE)
    
    ok <- tryCatch(
      {
        if (is.null(targets_to_run)) {
          message('Running full pipeline (mode = "', mode, '").')
          targets::tar_make_future(reporter = reporter)
        } else {
          message('Running selected targets (mode = "', mode, '") in parallel:')
          print(targets_to_run)
          
          rlang::inject(
            targets::tar_make_future(
              names = tidyselect::any_of(!!targets_to_run),
              reporter = reporter
            )
          )
        }
        TRUE
      },
      error = function(e) {
        warning(
          "Parallel run failed, falling back to sequential tar_make().\n",
          "Error: ", conditionMessage(e)
        )
        FALSE
      }
    )
    
    if (!isTRUE(ok)) {
      message("Using sequential execution via tar_make().")
      if (is.null(targets_to_run)) {
        targets::tar_make(reporter = reporter)
      } else {
        rlang::inject(
          targets::tar_make(
            names = tidyselect::any_of(!!targets_to_run),
            reporter = reporter
          )
        )
      }
    }
    
  } else {
    message("Using sequential execution via tar_make().")
    
    if (is.null(targets_to_run)) {
      message('Running full pipeline (mode = "', mode, '").')
      targets::tar_make(reporter = reporter)
    } else {
      message('Running selected targets (mode = "', mode, '"):')
      print(targets_to_run)
      
      # This avoids the tidyselect "external vector" warning.
      rlang::inject(
        targets::tar_make(
          names = tidyselect::any_of(!!targets_to_run),
          reporter = reporter
        )
      )
    }
  }
  
  # Print error summary (if any) right after the run.
  print_errored_targets_summary()
  
  # ---------------------------------------------------------------
  # Optional logging
  # ---------------------------------------------------------------
  if (isTRUE(log_run)) log_pipeline_run(mode = mode, targets_run = targets_to_run)
  
  # ---------------------------------------------------------------
  # Optionally load targets into the global environment
  # ---------------------------------------------------------------
  if (!isTRUE(load_interactively)) return(invisible(NULL))
  
  if (is.null(load_targets)) {
    load_targets <- if (!is.null(targets)) {
      targets_to_run
    } else {
      switch(
        mode,
        "all"       = load_presets$all,
        "main"      = load_presets$main,
        "load"      = load_presets$load,
        "translate" = load_presets$translate,
        "sdg"       = load_presets$sdg,
        "custom"    = targets_to_run,
        character(0)
      )
    }
  }
  
  clean_stale_targets_in_env(remove = TRUE, restrict_to = load_targets)
  load_targets_interactive(load_targets)
  invisible(NULL)
}
