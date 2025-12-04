# File: src/pipelines/03_sdg_detect/functions/10_input.R
# Purpose:
#   SDG detection (text2sdg) - input preparation helpers.
#
# Contains:
#   - sdg_to_id(): normalize SDG identifiers to "SDG-01".."SDG-17"
#   - build_sdg_input(): build a long table (document_number x section x text)
#     from guides_translated and sdg_detection config (combine_groups)
#
# Notes:
#   - Expects guides_translated to include `document_number`.
#   - combine_groups is read from config/sdg_detection.yml (sdg_cfg$combine_groups).

sdg_to_id <- function(x) {
  # Accepts "1" / "01" / "SDG-01" / etc and returns "SDG-01"
  num <- suppressWarnings(as.integer(stringr::str_extract(as.character(x), "\\d+")))
  ifelse(is.na(num), NA_character_, sprintf("SDG-%02d", num))
}

build_sdg_input <- function(guides_translated, sdg_cfg) {
  combine_groups <- sdg_cfg$combine_groups
  
  if (is.null(combine_groups) || !length(combine_groups)) {
    stop(
      "build_sdg_input(): missing `combine_groups:` in config/sdg_detection.yml.\n\n",
      "Please define at least one group, e.g.:\n",
      "combine_groups:\n",
      "  - id: \"course_info\"\n",
      "    prefix: \"Course information: \"\n",
      "   columns:\n",
      "     - course_description_en\n",
      "     - course_contents_en\n",
      call. = FALSE
    )
  }
  
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_sdg_input(): `guides_translated` must contain `document_number`.", call. = FALSE)
  }
  
  out_list <- list()
  
  for (grp in combine_groups) {
    id     <- grp$id
    prefix <- grp$prefix %||% ""
    cols   <- grp$columns %||% character(0)
    
    if (is.null(id) || !nzchar(id)) {
      stop("build_sdg_input(): each combine_groups entry must have a non-empty `id`.", call. = FALSE)
    }
    
    if (!length(cols)) {
      stop(
        "build_sdg_input(): combine_groups entry '", id, "' has no `columns`.\n",
        "Please define at least one column name.", call. = FALSE
      )
    }
    
    missing_cols <- setdiff(cols, names(guides_translated))
    if (length(missing_cols)) {
      stop(
        "build_sdg_input(): combine_groups entry '", id, "' references missing columns: ",
        paste(missing_cols, collapse = ", "), "\n",
        "Available columns: ", paste(names(guides_translated), collapse = ", "),
        call. = FALSE
      )
    }
    
    cols_existing <- intersect(cols, names(guides_translated))
    
    tmp <- guides_translated |>
      dplyr::select(document_number, dplyr::all_of(cols_existing))
    
    text_vec <- apply(
      tmp[, cols_existing, drop = FALSE],
      1,
      function(row) {
        pieces <- trimws(as.character(row))
        pieces <- pieces[!is.na(pieces) & pieces != ""]
        if (!length(pieces)) return(NA_character_)
        paste(pieces, collapse = " ||| ")
      }
    )
    
    if (nzchar(prefix)) {
      text_vec <- ifelse(
        is.na(text_vec) | text_vec == "",
        NA_character_,
        paste0(prefix, text_vec)
      )
    }
    
    df_grp <- tibble::tibble(
      document_number = guides_translated$document_number,
      section         = id,
      text            = text_vec
    ) |>
      dplyr::filter(!is.na(text) & text != "")
    
    out_list[[id]] <- df_grp
  }
  
  sdg_input <- dplyr::bind_rows(out_list)
  
  if (!nrow(sdg_input)) {
    stop(
      "build_sdg_input(): no non-empty text found for any combine_groups. ",
      "Check that the *_en columns referenced in combine_groups exist and are not all empty.",
      call. = FALSE
    )
  }
  
  sdg_input
}
