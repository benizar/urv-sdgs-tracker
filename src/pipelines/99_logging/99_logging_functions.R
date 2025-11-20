# File: src/pipelines/99_logging/99_logging_functions.R
# Helpers to read and summarise the {targets} run log.
#
# The log file itself is written by run_pipeline() via log_pipeline_run().
# These helpers are used by 99_logging targets to analyse past runs.

get_log_path <- function() {
  file.path("sandbox", "logs", "targets_log.csv")
}

# Safe reader: if the log file does not exist yet,
# return an empty tibble instead of failing.
read_log_safe <- function() {
  path <- get_log_path()
  
  if (!file.exists(path)) {
    message("read_log_safe(): log file not found, returning empty table.")
    return(tibble::tibble())
  }
  
  readr::read_csv(path, show_col_types = FALSE)
}

summarise_log_by_target <- function(log_tbl) {
  if (!nrow(log_tbl)) {
    message("summarise_log_by_target(): empty log, returning empty summary.")
    return(tibble::tibble())
  }
  
  log_tbl %>%
    dplyr::group_by(name) %>%
    dplyr::summarise(
      n_runs      = dplyr::n(),
      total_secs  = sum(seconds, na.rm = TRUE),
      mean_secs   = mean(seconds, na.rm = TRUE),
      last_run    = max(finished, na.rm = TRUE),
      any_error   = any(!is.na(error) & nzchar(error)),
      any_warning = any(!is.na(warning) & nzchar(warning)),
      .groups     = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(last_run))
}

summarise_log_by_run <- function(log_tbl) {
  if (!nrow(log_tbl)) {
    message("summarise_log_by_run(): empty log, returning empty summary.")
    return(tibble::tibble())
  }
  
  log_tbl %>%
    dplyr::group_by(run_timestamp, run_mode, run_targets_requested) %>%
    dplyr::summarise(
      n_targets   = dplyr::n(),
      total_secs  = sum(seconds, na.rm = TRUE),
      max_secs    = max(seconds, na.rm = TRUE),
      any_error   = any(!is.na(error) & nzchar(error)),
      any_warning = any(!is.na(warning) & nzchar(warning)),
      .groups     = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(run_timestamp))
}
