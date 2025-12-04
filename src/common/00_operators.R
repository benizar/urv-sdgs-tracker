# File: src/common/00_operators.R
# Operators and nano-helpers used across the pipeline.

# Null-coalescing-like operator for default values.
# Usage:
#   cfg$run_id %||% "20251115"
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
