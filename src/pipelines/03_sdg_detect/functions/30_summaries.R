# File: src/pipelines/03_sdg_detect/functions/30_summaries.R
# Purpose:
#   SDG detection (text2sdg) - post-processing helpers.
#
# Contains:
#   - summarise_sdg_hits_long(): one row per (document_number, section, system, sdg)
#   - summarise_sdg_hits_wide(): wide matrix for modeling / exports
#   - attach_sdg_to_guides(): attach wide SDG fields back to guides_translated

summarise_sdg_hits_long <- function(hits) {
  if (!nrow(hits)) {
    return(
      tibble::tibble(
        document_number = character(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        n_hits          = integer(0),
        sdg_num         = character(0)
      )
    )
  }
  
  hits |>
    dplyr::mutate(
      sdg = as.character(sdg),
      sdg_num = stringr::str_pad(
        stringr::str_extract(sdg, "\\d+"),
        width = 2,
        pad = "0"
      )
    ) |>
    dplyr::group_by(document_number, section, system, sdg, sdg_num) |>
    dplyr::summarise(n_hits = dplyr::n(), .groups = "drop")
}

summarise_sdg_hits_wide <- function(sdg_long, sdg_cfg) {
  if (!nrow(sdg_long)) {
    return(tibble::tibble(document_number = character(0)))
  }
  
  agg_mode <- sdg_cfg$aggregate$mode %||% "counts"
  min_hits <- as.integer(sdg_cfg$aggregate$min_hits %||% 1L)
  if (is.na(min_hits) || min_hits < 1L) min_hits <- 1L
  
  agg_mode <- tolower(agg_mode)
  if (!agg_mode %in% c("counts", "binary")) {
    stop("summarise_sdg_hits_wide(): invalid aggregate.mode: ", agg_mode, call. = FALSE)
  }
  
  sdg_long |>
    dplyr::mutate(
      value = if (identical(agg_mode, "binary")) as.integer(n_hits >= min_hits) else n_hits,
      colname = paste0("sdg_", sdg_num, "_", system, "_", section)
    ) |>
    dplyr::select(document_number, colname, value) |>
    tidyr::pivot_wider(
      id_cols     = document_number,
      names_from  = colname,
      values_from = value,
      values_fill = 0
    )
}

attach_sdg_to_guides <- function(guides_translated, sdg_hits_wide) {
  guides_translated |>
    dplyr::left_join(sdg_hits_wide, by = "document_number")
}
