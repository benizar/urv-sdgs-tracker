# File: src/pipelines/02_clean/02_clean_targets.R
# Targets for the cleaning phase of the URV SDGs tracker pipeline.
#
# CURRENT BEHAVIOUR:
#   - Takes guides_index (from 01_import) as input.
#   - Produces guides_clean with additional *_clean columns for
#     course name and long text fields, and normalised numeric types.
#
# FUTURE BEHAVIOUR:
#   - Additional cleaning targets can be added here, for example:
#       * guides_clean_flags (quality indicators),
#       * guides_clean_tokens (pre-tokenised text for SDG models).

library(targets)

targets_clean <- list(
  tar_target(
    guides_clean,
    clean_guides_index(guides_index)
  )
)
