
# -------------------------------------------------------------------
# SDG labels (external resource)
# -------------------------------------------------------------------
# Reads a CSV with SDG labels in multiple languages to avoid hardcoding.
#
# Expected minimum column:
#   - sdg_id (e.g. "SDG-01")
#
# Recommended columns (any subset is fine):
#   - label_ca, label_es, label_en, ...
# Optionally:
#   - sdg_num (1..17) (not required)
#
# The function returns a tibble with:
#   sdg_id + ods_label_* columns (renamed from label_*)
read_sdg_labels <- function(path = "resources/sdg_labels.csv") {
  if (!file.exists(path)) {
    stop("read_sdg_labels(): SDG labels file not found: ", path, call. = FALSE)
  }
  
  # Try ';' first (common in this project), then ',' as fallback.
  df <- tryCatch(
    readr::read_delim(path, delim = ";", show_col_types = FALSE, progress = FALSE),
    error = function(e) NULL
  )
  if (is.null(df)) {
    df <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE)
  }
  
  if (!"sdg_id" %in% names(df)) {
    stop("read_sdg_labels(): labels CSV must include a 'sdg_id' column.", call. = FALSE)
  }
  
  df <- df |>
    dplyr::mutate(sdg_id = as.character(sdg_id)) |>
    dplyr::filter(!is.na(sdg_id) & nzchar(sdg_id))
  
  # Normalize labels naming: label_xx -> ods_label_xx
  label_cols <- grep("^label_[a-z]{2,}$", names(df), value = TRUE)
  if (length(label_cols)) {
    df <- df |>
      dplyr::rename_with(
        .fn = function(nm) sub("^label_", "ods_label_", nm),
        .cols = dplyr::all_of(label_cols)
      )
  }
  
  # Keep stable join key, plus any language labels (already normalized),
  # plus any auxiliary columns you may want later (e.g., sdg_num).
  keep_cols <- c(
    "sdg_id",
    intersect(c("sdg_num"), names(df)),
    grep("^ods_label_[a-z]{2,}$", names(df), value = TRUE)
  )
  keep_cols <- unique(keep_cols)
  df <- df |>
    dplyr::select(dplyr::all_of(keep_cols)) |>
    dplyr::distinct(sdg_id, .keep_all = TRUE)
  
  df
}