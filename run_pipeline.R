# File: run_pipeline.R
# Helper to run and inspect the URV SDGs tracker pipeline from RStudio.

library(targets)
library(yaml)
library(future)
library(rlang)

# -------------------------------------------------------------------
# Logging helpers
# -------------------------------------------------------------------

get_pipeline_log_path <- function() {
  file.path("sandbox", "logs", "targets_log.csv")
}

log_pipeline_run <- function(mode, targets_run, log_path = get_pipeline_log_path()) {
  meta <- targets::tar_meta(
    fields = c(name, started, time, seconds, bytes, error, warning, pattern, iteration)
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

get_pipeline_target_names <- function() {
  if (exists("tar_manifest", where = asNamespace("targets"), inherits = FALSE)) {
    man <- tryCatch(targets::tar_manifest(fields = name), error = function(e) NULL)
    if (!is.null(man) && nrow(man)) return(as.character(man$name))
  }
  
  meta <- tryCatch(targets::tar_meta(fields = name), error = function(e) NULL)
  if (!is.null(meta) && nrow(meta)) return(as.character(meta$name))
  
  character(0)
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
# Remake helpers (version-compatible)
# -------------------------------------------------------------------

remake_all_targets <- function() {
  targets::tar_destroy(destroy = "all")
  invisible(TRUE)
}

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
  if (!file.exists(cfg_path)) return(list(enabled = FALSE, backend = "future", workers = 1L))
  
  cfg <- yaml::read_yaml(cfg_path)
  parallel_cfg <- cfg$global$parallel
  
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
  meta <- tryCatch(
    targets::tar_meta(fields = c(name, error)),
    error = function(e) NULL
  )
  
  if (is.null(meta) || !is.data.frame(meta) || nrow(meta) == 0) {
    return(invisible(NULL))
  }
  
  err <- dplyr::filter(meta, !is.na(error) & nzchar(error))
  if (nrow(err) == 0) return(invisible(NULL))
  
  message("Some targets errored:")
  print(err, row.names = FALSE)
  invisible(NULL)
}


# -------------------------------------------------------------------
# Main entry point to run the pipeline
# -------------------------------------------------------------------

run_pipeline <- function(
    mode = c("all", "main", "load", "custom"),
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
    targets_to_run <- unique(as.character(targets))
    message(
      "run_pipeline(): using explicit targets for execution: ",
      paste(targets_to_run, collapse = ", ")
    )
  } else {
    targets_to_run <- switch(
      mode,
      "all"  = NULL,
      "main" = NULL,
      "load" = c(
        "centres_list",
        "programmes_list",
        "course_details_list",
        "guido_docnet_course_code_map",
        "docnet_course_info",
        "guido_course_info",
        "guides_index"
      ),
      "custom" = stop('run_pipeline(mode = "custom") requires a non-NULL `targets` argument.')
    )
  }
  
  # ---------------------------------------------------------------
  # Optional remake
  # ---------------------------------------------------------------
  if (isTRUE(remake)) {
    if (is.null(targets_to_run)) {
      message("run_pipeline(): remake = TRUE and full pipeline selected. Destroying ALL targets.")
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
  # Decide sequential vs parallel
  # ---------------------------------------------------------------
  parallel_cfg <- get_parallel_config()
  
  if (is.null(use_parallel)) {
    use_parallel_flag <- parallel_cfg$enabled
  } else {
    use_parallel_flag <- isTRUE(use_parallel)
  }
  
  if (use_parallel_flag) {
    if (!exists("tar_make_future", where = asNamespace("targets"), inherits = FALSE)) {
      warning(
        "Parallel execution requested, but tar_make_future() is not available.\n",
        "Falling back to tar_make() (sequential)."
      )
      use_parallel_flag <- FALSE
    }
  }
  
  # ---------------------------------------------------------------
  # Run the pipeline
  # ---------------------------------------------------------------
  if (use_parallel_flag) {
    message("Using parallel execution via tar_make_future() with ", parallel_cfg$workers, " workers.")
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
      targets::tar_make(names = targets_to_run)    }
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
        "all"  = get_pipeline_target_names(),
        "main" = c(
          "guides_index",
          "guides_translated",
          "sdg_config",
          "sdg_input",
          "sdg_hits_long",
          "sdg_hits_wide",
          "guides_sdg"
        ),
        "load"   = c("docnet_course_info", "guido_course_info", "guides_index"),
        "custom" = targets_to_run,
        character(0)
      )
    }
  }
  
  clean_stale_targets_in_env(remove = TRUE, restrict_to = load_targets)
  load_targets_interactive(load_targets)
  invisible(NULL)
}
