# File: src/common/common_utils.R
# Common helper functions and operators used across the pipeline.

# Null-coalescing-like operator for default values.
# Usage:
#   cfg$run_id %||% "20251115"
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
