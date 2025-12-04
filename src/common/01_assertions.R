# File: src/common/01_assertions.R

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
