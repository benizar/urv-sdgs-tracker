# File: src/common/join_helpers.R
# Join helpers for pipeline tables.
#
# Goals:
#   - Prevent accidental row multiplication (enforce unique keys on the RHS).
#   - Fail early with clear messages when keys are missing, NA, or duplicated.
#   - Provide helpers to "unify" homonymous GUIdO/DOCnet columns into one final column
#     (optionally keeping the source-specific columns).
#   - Provide DOCnet-specific fusers (e.g., merge multiple competences/learning-results columns).
#
# Notes:
#   - These helpers assume tidyverse conventions (dplyr/rlang).
#   - They are side-effect free (return new data.frames).
#   - They are intended for use inside target functions.

# -------------------------------------------------------------------
# Basic validators
# -------------------------------------------------------------------

assert_has_cols <- function(df, cols, df_name = deparse(substitute(df))) {
  if (!is.data.frame(df)) stop("`df` must be a data.frame.")
  cols <- as.character(cols)
  missing <- setdiff(cols, names(df))
  if (length(missing) > 0) {
    stop(
      "Missing expected column(s) in ", df_name, ": ",
      paste(missing, collapse = ", ")
    )
  }
  invisible(TRUE)
}

assert_no_na_key <- function(df, key, df_name = deparse(substitute(df))) {
  assert_has_cols(df, key, df_name = df_name)
  key <- as.character(key)
  
  is_bad <- df |>
    dplyr::transmute(
      .bad = dplyr::if_all(dplyr::all_of(key), ~ is.na(.x) | !nzchar(as.character(.x)))
    ) |>
    dplyr::pull(.bad)
  
  if (any(is_bad)) {
    bad_n <- sum(is_bad)
    stop(
      "Key contains NA/empty values in ", df_name, " for: ", paste(key, collapse = ", "),
      "\nBad rows: ", bad_n
    )
  }
  invisible(TRUE)
}

assert_unique_key <- function(df, key, df_name = deparse(substitute(df)), n_show = 10) {
  assert_has_cols(df, key, df_name = df_name)
  key <- as.character(key)
  
  dup <- df |>
    dplyr::count(dplyr::across(dplyr::all_of(key)), name = "n") |>
    dplyr::filter(.data$n > 1)
  
  if (nrow(dup) > 0) {
    preview <- utils::capture.output(utils::head(dup, n_show))
    stop(
      "Key is not unique in ", df_name,
      " for: ", paste(key, collapse = ", "),
      "\nExample duplicates:\n",
      paste(preview, collapse = "\n")
    )
  }
  invisible(TRUE)
}

# Optional: checks that an intended 1:1 mapping table is truly 1:1 (both sides unique).
assert_one_to_one_map <- function(df,
                                  left_key,
                                  right_key,
                                  df_name = deparse(substitute(df))) {
  assert_no_na_key(df, left_key, df_name = df_name)
  assert_no_na_key(df, right_key, df_name = df_name)
  assert_unique_key(df, left_key, df_name = paste0(df_name, " (left side)"))
  assert_unique_key(df, right_key, df_name = paste0(df_name, " (right side)"))
  invisible(TRUE)
}

# -------------------------------------------------------------------
# Safe joins (enforce RHS key uniqueness)
# -------------------------------------------------------------------

safe_left_join <- function(x,
                           y,
                           by,
                           y_name = deparse(substitute(y)),
                           allow_na_y_key = FALSE) {
  if (!is.data.frame(x) || !is.data.frame(y)) stop("`x` and `y` must be data.frames.")
  if (!is.character(by) || is.null(names(by)) || any(!nzchar(names(by))) || any(!nzchar(by))) {
    stop("`by` must be a named character vector like c('x_key' = 'y_key').")
  }
  
  y_key <- unname(by)
  assert_has_cols(y, y_key, df_name = y_name)
  if (!allow_na_y_key) assert_no_na_key(y, y_key, df_name = y_name)
  assert_unique_key(y, y_key, df_name = y_name)
  
  dplyr::left_join(x, y, by = by)
}

safe_inner_join <- function(x,
                            y,
                            by,
                            y_name = deparse(substitute(y)),
                            allow_na_y_key = FALSE) {
  if (!is.data.frame(x) || !is.data.frame(y)) stop("`x` and `y` must be data.frames.")
  if (!is.character(by) || is.null(names(by)) || any(!nzchar(names(by))) || any(!nzchar(by))) {
    stop("`by` must be a named character vector like c('x_key' = 'y_key').")
  }
  
  y_key <- unname(by)
  assert_has_cols(y, y_key, df_name = y_name)
  if (!allow_na_y_key) assert_no_na_key(y, y_key, df_name = y_name)
  assert_unique_key(y, y_key, df_name = y_name)
  
  dplyr::inner_join(x, y, by = by)
}

# -------------------------------------------------------------------
# Text fusers (useful for DOCnet multi-columns)
# -------------------------------------------------------------------

collapse_text_uniques <- function(..., sep = ";\n") {
  xs <- list(...)
  xs <- unlist(xs, use.names = FALSE)
  
  xs <- xs[!is.na(xs)]
  xs <- trimws(as.character(xs))
  xs <- xs[nzchar(xs)]
  xs <- unique(xs)
  
  if (length(xs) == 0) return(NA_character_)
  paste(xs, collapse = sep)
}

# Fuse several columns into one (rowwise), keeping unique non-empty items.
# Example:
#   df <- fuse_text_columns(df, out = "course_competences",
#                           cols = c("course_competences", "course_competences_2"))
fuse_text_columns <- function(df, out, cols, sep = ";\n", drop_inputs = TRUE) {
  if (!is.data.frame(df)) stop("`df` must be a data.frame.")
  if (!is.character(out) || length(out) != 1 || !nzchar(out)) stop("`out` must be a non-empty string.")
  cols <- as.character(cols)
  
  existing <- intersect(cols, names(df))
  if (length(existing) == 0) return(df) # nothing to do
  
  df2 <- df |>
    dplyr::rowwise() |>
    dplyr::mutate(!!out := collapse_text_uniques(!!!rlang::syms(existing), sep = sep)) |>
    dplyr::ungroup()
  
  if (drop_inputs) {
    df2 <- df2 |> dplyr::select(-dplyr::all_of(setdiff(existing, out)))
  }
  
  df2
}

# DOCnet convenience wrapper:
# - merges multiple "competences" columns into "course_competences"
# - merges multiple "learning results" columns into "course_learning_results"
# Adjust the candidate column names here to match your scrape exports.
normalize_docnet_variants <- function(df, sep = ";\n") {
  if (!is.data.frame(df)) stop("`df` must be a data.frame.")
  
  df |>
    fuse_text_columns(
      out  = "course_competences",
      cols = c("course_competences", "course_competences_2", "course_competences_alt"),
      sep = sep,
      drop_inputs = TRUE
    ) |>
    fuse_text_columns(
      out  = "course_learning_results",
      cols = c("course_learning_results", "course_learning_results_2", "course_learning_results_alt"),
      sep = sep,
      drop_inputs = TRUE
    )
}

# -------------------------------------------------------------------
# Course-info unifier (GUIdO + DOCnet -> unified "course_*" columns)
# -------------------------------------------------------------------

# Create unified columns from two sources already present in the same df.
#
# Expected pattern:
#   - Source-specific columns exist as: <field><suffix_guido> and <field><suffix_docnet>
#   - You want a final column named exactly <field>
#
# prefer:
#   - "guido": course_field := coalesce(course_field_guido, course_field_docnet)
#   - "docnet": course_field := coalesce(course_field_docnet, course_field_guido)
#
# keep_sources:
#   - TRUE: keep *_guido/*_docnet columns too
#   - FALSE: drop them after unifying
unify_course_fields <- function(df,
                                fields,
                                suffix_guido = "_guido",
                                suffix_docnet = "_docnet",
                                prefer = c("guido", "docnet"),
                                keep_sources = TRUE) {
  if (!is.data.frame(df)) stop("`df` must be a data.frame.")
  fields <- as.character(fields)
  prefer <- match.arg(prefer)
  
  for (f in fields) {
    g <- paste0(f, suffix_guido)
    d <- paste0(f, suffix_docnet)
    
    has_g <- g %in% names(df)
    has_d <- d %in% names(df)
    
    if (!has_g && !has_d) next
    
    df[[f]] <- dplyr::case_when(
      prefer == "guido"  ~ dplyr::coalesce(df[[g]], df[[d]]),
      prefer == "docnet" ~ dplyr::coalesce(df[[d]], df[[g]])
    )
  }
  
  if (!isTRUE(keep_sources)) {
    drop_cols <- c(paste0(fields, suffix_guido), paste0(fields, suffix_docnet))
    drop_cols <- intersect(drop_cols, names(df))
    df <- df |> dplyr::select(-dplyr::all_of(drop_cols))
  }
  
  df
}

# Helper to rename all course_* columns (except id columns) with a suffix.
suffix_course_columns <- function(df, suffix, id_cols) {
  if (!is.data.frame(df)) stop("`df` must be a data.frame.")
  if (!is.character(suffix) || length(suffix) != 1) stop("`suffix` must be a string.")
  id_cols <- as.character(id_cols)
  assert_has_cols(df, id_cols)
  
  to_suffix <- setdiff(names(df), id_cols)
  df |> dplyr::rename_with(~ paste0(.x, suffix), dplyr::all_of(to_suffix))
}
