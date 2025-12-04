# File: src/common/03_text_utils.R
# Purpose:
#   Small, reusable text utilities used across the pipeline.
#   These helpers standardise how we clean, de-duplicate and concatenate
#   text fragments coming from multiple columns or sources.
#
# Contents:
#   - collapse_unique_text(): clean + unique (+ optional sort) + paste()

# Collapse values into a single string after cleaning.
#
# What it does:
#   1) flattens inputs (vectors or multiple arguments)
#   2) coerces to character
#   3) drops NA, trims whitespace, drops empty strings
#   4) keeps unique values
#   5) optionally sorts them
#   6) concatenates using `sep`
#
# Parameters:
#   ...         One or more vectors / values to collapse.
#   sep         Separator used in the final paste.
#   sort        If TRUE, sort unique values (useful for deterministic outputs).
#   empty       Value to return when nothing remains after cleaning.
#               Use "" where you want an empty string, or NA_character_ where
#               you want missingness to propagate.
collapse_unique_text <- function(..., sep = ";\n", sort = TRUE, empty = NA_character_) {
  xs <- list(...)
  xs <- unlist(xs, use.names = FALSE)
  
  xs <- xs[!is.na(xs)]
  xs <- trimws(as.character(xs))
  xs <- xs[nzchar(xs)]
  xs <- unique(xs)
  
  if (!length(xs)) return(empty)
  if (isTRUE(sort)) xs <- sort(xs)
  
  paste(xs, collapse = sep)
}
