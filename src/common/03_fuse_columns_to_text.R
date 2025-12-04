# File: src/common/03_fuse_columns_to_text.R
# Purpose:
#   Generic helpers to fuse multiple text columns into a single string (or tibble)
#   using configurable groups (prefix + columns) and a standard cleaning strategy.
#
# Typical uses:
#   - Build sectioned inputs for NLP / classification (e.g., SDG detection)
#   - Create readable "combined text" fields for review/export

coerce_to_text_default <- function(x) {
  # Lists: paste atomic items, otherwise deparse
  if (is.list(x) && !is.data.frame(x)) {
    return(vapply(x, function(e) {
      if (is.null(e) || length(e) == 0) return(NA_character_)
      if (is.atomic(e)) return(paste(e, collapse = ", "))
      paste(capture.output(str(e)), collapse = " ")
    }, character(1)))
  }
  as.character(x)
}

fuse_columns_to_text <- function(df,
                                 cols,
                                 out,
                                 labels = NULL,
                                 sep_blocks = "\n\n",
                                 sep_label_value = ":\n",
                                 drop_inputs = FALSE,
                                 coerce = TRUE,
                                 formatter = NULL,
                                 trim = TRUE,
                                 empty_to_na = TRUE) {
  if (!is.data.frame(df)) stop("`df` must be a data.frame.")
  cols <- as.character(cols)
  if (length(cols) < 1) stop("`cols` must have at least one column name.")
  if (!is.character(out) || length(out) != 1 || !nzchar(out)) stop("`out` must be a non-empty string.")
  
  missing <- setdiff(cols, names(df))
  if (length(missing) > 0) stop("Missing column(s) in df: ", paste(missing, collapse = ", "))
  
  if (is.null(labels)) labels <- cols
  labels <- as.character(labels)
  if (length(labels) != length(cols)) stop("`labels` must have the same length as `cols` (or be NULL).")
  
  parts <- df[, cols, drop = FALSE]
  
  # Convert each column to text
  parts <- lapply(parts, function(x) {
    if (!is.null(formatter)) {
      x <- formatter(x)
    } else if (isTRUE(coerce)) {
      x <- coerce_to_text_default(x)
    } else {
      if (!is.character(x)) stop("Non-character column encountered and `coerce = FALSE`.")
      x
    }
    
    if (isTRUE(trim)) x <- trimws(x)
    if (isTRUE(empty_to_na)) x[x == ""] <- NA_character_
    x
  })
  parts <- as.data.frame(parts, stringsAsFactors = FALSE)
  
  df[[out]] <- purrr::pmap_chr(parts, function(...) {
    xs <- list(...)
    keep <- !vapply(xs, function(z) is.na(z) || !nzchar(z), logical(1))
    xs <- xs[keep]
    if (length(xs) == 0) return(NA_character_)
    labs <- labels[keep]
    blocks <- Map(function(lab, val) paste0(lab, sep_label_value, val), labs, xs)
    paste(blocks, collapse = sep_blocks)
  })
  
  if (isTRUE(drop_inputs)) {
    df <- df |> dplyr::select(-dplyr::all_of(cols))
  }
  
  df
}
