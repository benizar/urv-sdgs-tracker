# File: src/pipelines/99_logging/99_logging_functions.R
# Helpers for analysing the pipeline log written by run_pipeline().

# Centralised path: must match run_pipeline::get_pipeline_log_path()
get_log_path <- function() {
  file.path("sandbox", "logs", "targets_log.csv")
}

# Load the accumulated pipeline log from CSV.
load_pipeline_log <- function(path = get_log_path()) {
  if (!file.exists(path)) {
    message(
      "Log file not found: ", path,
      " – returning empty log table (run_pipeline() will create it ",
      "after the first successful run)."
    )
    
    # Return an empty tibble with the expected columns.
    return(
      tibble::tibble(
        name                  = character(0),
        started               = as.POSIXct(character(0)),
        time                  = numeric(0),
        seconds               = numeric(0),
        bytes                 = numeric(0),
        error                 = character(0),
        warning               = character(0),
        pattern               = character(0),
        iteration             = character(0),
        run_timestamp         = as.POSIXct(character(0)),
        run_mode              = character(0),
        run_targets_requested = character(0)
      )
    )
  }
  
  df <- readr::read_csv(path, show_col_types = FALSE)
  
  # Basic sanity checks (do not stop, only warn)
  required_cols <- c("name", "started", "seconds", "run_timestamp")
  missing_cols  <- setdiff(required_cols, names(df))
  if (length(missing_cols)) {
    warning(
      "load_pipeline_log(): log file is missing columns: ",
      paste(missing_cols, collapse = ", "),
      ". Some summaries may be limited."
    )
  }
  
  df
}

# Summarise log by target:
#   - one row per target name
#   - number of runs
#   - last_run (based on `started`)
#   - average runtime in seconds
#   - last error / warning messages if available
summarise_log_by_target <- function(log_df) {
  if (!nrow(log_df)) {
    return(
      tibble::tibble(
        name      = character(0),
        n_runs    = integer(0),
        last_run  = as.POSIXct(character(0)),
        avg_sec   = numeric(0),
        last_err  = character(0),
        last_warn = character(0)
      )
    )
  }
  
  # If there is no `name` column, we cannot summarise by target.
  # This typically happens with legacy log files.
  if (!"name" %in% names(log_df)) {
    warning(
      "summarise_log_by_target(): 'name' column is missing in log_df; ",
      "returning empty summary."
    )
    return(
      tibble::tibble(
        name      = character(0),
        n_runs    = integer(0),
        last_run  = as.POSIXct(character(0)),
        avg_sec   = numeric(0),
        last_err  = character(0),
        last_warn = character(0)
      )
    )
  }
  
  # Fallbacks in case some columns are absent
  if (!"started" %in% names(log_df)) {
    if ("run_timestamp" %in% names(log_df)) {
      log_df$started <- log_df$run_timestamp
    } else {
      log_df$started <- as.POSIXct(NA)
    }
  }
  if (!"seconds" %in% names(log_df)) {
    log_df$seconds <- NA_real_
  }
  if (!"error" %in% names(log_df)) {
    log_df$error <- NA_character_
  }
  if (!"warning" %in% names(log_df)) {
    log_df$warning <- NA_character_
  }
  
  log_df %>%
    dplyr::group_by(name) %>%
    dplyr::summarise(
      n_runs   = dplyr::n(),
      last_run = suppressWarnings(max(started, na.rm = TRUE)),
      avg_sec  = mean(seconds, na.rm = TRUE),
      last_err = {
        errs <- stats::na.omit(error)
        if (length(errs)) tail(errs, 1L) else NA_character_
      },
      last_warn = {
        warns <- stats::na.omit(warning)
        if (length(warns)) tail(warns, 1L) else NA_character_
      },
      .groups = "drop"
    )
}
