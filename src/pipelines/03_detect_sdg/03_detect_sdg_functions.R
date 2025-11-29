# File: src/pipelines/03_detect_sdg/03_detect_sdg_functions.R
#
# SDG detection helpers for the URV SDGs tracker pipeline.
#
# Responsibilities:
#   - build the SDG input table (document_number x section x text)
#     from the translated guides table (guides_translated)
#   - run text2sdg (systems or ensemble)
#   - summarise hits (long and wide)
#   - attach SDG summaries back to the guides table
#
# NOTE:
#   - guides_translated is produced by the translation phase, which
#     can run in "auto" or "reviewer" mode. From the SDG point of view,
#     it is always just a data frame with *_en columns.
#   - attach_sdg_to_guides() only joins sdg_hits_wide back to whatever
#     guides table you pass in (typically guides_translated).

# -------------------------------------------------------------------
# Build SDG input table from a translated guides table and sdg config
# -------------------------------------------------------------------

# Output:
#   - one row per (document_number, section)
#   - columns: document_number, section, text
#
# Sections are defined in pipeline.yml under:
#   sdg_detection:
#     combine_groups:
#       - id: "course_info"
#         prefix: "Course information: "
#         columns: [ ... *_en ]
build_sdg_input <- function(guides_translated, sdg_cfg) {
  combine_groups <- sdg_cfg$combine_groups
  
  if (is.null(combine_groups) || !length(combine_groups)) {
    stop(
      "build_sdg_input(): missing `sdg_detection: combine_groups:` in config/pipeline.yml.\n\n",
      "Please define at least one group, e.g.:\n",
      "sdg_detection:\n",
      "  combine_groups:\n",
      "    - id: \"course_info\"\n",
      "      prefix: \"Course information: \"\n",
      "      columns:\n",
      "        - course_description_en\n",
      "        - course_contents_en\n",
      call. = FALSE
    )
  }
  
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_sdg_input(): `guides_translated` must contain `document_number`.", call. = FALSE)
  }
  
  # ------------------------------------------------------------------
  # Normal path: one row per (document_number, combine_group$id)
  # ------------------------------------------------------------------
  out_list <- list()
  
  for (grp in combine_groups) {
    id     <- grp$id
    prefix <- grp$prefix %||% ""
    cols   <- grp$columns %||% character(0)
    
    if (is.null(id) || !nzchar(id)) {
      stop(
        "build_sdg_input(): combine_groups entries must include ",
        "a non-empty 'id' field.",
        call. = FALSE
      )
    }
    
    if (!length(cols)) {
      stop(
        "build_sdg_input(): combine_groups entry '", id,
        "' has no columns.",
        call. = FALSE
      )
    }
    
    cols_existing <- intersect(cols, names(guides_translated))
    missing_cols  <- setdiff(cols, cols_existing)
    
    if (length(missing_cols)) {
      stop(
        "build_sdg_input(): combine_groups '", id,
        "' refers to missing columns: ",
        paste(missing_cols, collapse = ", "),
        call. = FALSE
      )
    }
    
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
      "Check that the *_en columns referenced in combine_groups exist ",
      "and are not all empty.",
      call. = FALSE
    )
  }
  
  sdg_input
}

# -------------------------------------------------------------------
# Run text2sdg with configurable options
# -------------------------------------------------------------------

run_text2sdg_detection <- function(sdg_input, sdg_cfg) {
  method  <- sdg_cfg$method  %||% "systems"
  sdgs    <- sdg_cfg$sdgs    %||% 1:17
  verbose <- sdg_cfg$verbose %||% FALSE
  
  method <- tolower(method)
  
  if (!nrow(sdg_input)) {
    return(
      tibble::tibble(
        document_number = character(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        document        = integer(0)
      )
    )
  }
  
  if (identical(method, "systems")) {
    systems <- sdg_cfg$systems %||% c("Aurora", "Elsevier", "Auckland", "SIRIS")
    output  <- sdg_cfg$output  %||% "documents"
    
    # text2sdg::detect_sdg_systems() signature:
    #   detect_sdg_systems(text, system, sdgs, output, verbose, ...)
    hits <- text2sdg::detect_sdg_systems(
      text    = sdg_input$text,
      system  = systems,
      sdgs    = sdgs,
      output  = output,
      verbose = verbose
    )
    
  } else if (identical(method, "ensemble")) {
    # text2sdg::detect_sdg() signature:
    #   detect_sdg(text, sdgs, verbose, ...)
    hits <- text2sdg::detect_sdg(
      text    = sdg_input$text,
      sdgs    = sdgs,
      verbose = verbose
    )
    
    # Ensure a 'system' column exists downstream
    if (!"system" %in% names(hits)) {
      hits$system <- "Ensemble"
    }
    
  } else {
    stop("run_text2sdg_detection(): unknown method in config: ", method, call. = FALSE)
  }
  
  if (!nrow(hits)) {
    message("run_text2sdg_detection(): text2sdg returned 0 hits.")
    hits$document_number <- character(0)
    hits$section         <- character(0)
    return(hits)
  }
  
  if (!"document" %in% names(hits)) {
    stop("run_text2sdg_detection(): text2sdg output does not include 'document' column.", call. = FALSE)
  }
  
  doc_index <- as.integer(hits$document)
  doc_map   <- sdg_input$document_number
  sec_map   <- sdg_input$section
  
  if (any(is.na(doc_index)) ||
      any(doc_index < 1L) ||
      any(doc_index > length(doc_map))) {
    stop("run_text2sdg_detection(): invalid 'document' indices in hits.", call. = FALSE)
  }
  
  hits |>
    dplyr::mutate(
      document_number = doc_map[doc_index],
      section         = sec_map[doc_index],
      sdg             = as.character(sdg)
    )
}

# -------------------------------------------------------------------
# Long summary: document_number x section x system x sdg
# -------------------------------------------------------------------

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
  
  if (!"document_number" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'document_number' column is required.", call. = FALSE)
  }
  if (!"section" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'section' column is required.", call. = FALSE)
  }
  if (!"system" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'system' column is required.", call. = FALSE)
  }
  if (!"sdg" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'sdg' column is required.", call. = FALSE)
  }
  
  hits |>
    dplyr::mutate(sdg = as.character(sdg)) |>
    dplyr::group_by(
      document_number,
      section,
      system,
      sdg
    ) |>
    dplyr::summarise(
      n_hits = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      sdg_num = stringr::str_extract(sdg, "\\d+")
    )
}

# -------------------------------------------------------------------
# Wide summary: 1 row per document_number, 1 col per (section, system, sdg)
# -------------------------------------------------------------------

summarise_sdg_hits_wide <- function(sdg_long, sdg_cfg) {
  if (!nrow(sdg_long)) {
    return(
      tibble::tibble(
        document_number = character(0)
      )
    )
  }
  
  agg_mode <- sdg_cfg$aggregate$mode     %||% "counts"
  min_hits <- sdg_cfg$aggregate$min_hits %||% 1L
  
  sdg_long |>
    dplyr::mutate(
      value = if (identical(agg_mode, "binary")) {
        as.integer(n_hits >= min_hits)
      } else {
        n_hits
      },
      colname = paste0(
        "sdg_",
        sdg_num, "_",
        system, "_",
        section
      )
    ) |>
    dplyr::select(
      document_number,
      colname,
      value
    ) |>
    tidyr::pivot_wider(
      id_cols      = document_number,
      names_from   = colname,
      values_from  = value,
      values_fill  = 0
    )
}

# -------------------------------------------------------------------
# Attach SDG wide summary back to the guides table
# -------------------------------------------------------------------

attach_sdg_to_guides <- function(guides_translated, sdg_hits_wide) {
  guides_translated |>
    dplyr::left_join(sdg_hits_wide, by = "document_number")
}
