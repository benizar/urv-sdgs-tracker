# File: src/pipelines/04_sdg/04_detect_sdg_functions.R

# Build SDG input table from guides_translated and sdg config.
#
# Output:
#   - one row per (document_number, section)
#   - columns: document_number, section, text
#
# Sections are defined in pipeline.yml under:
#   sdg_detection:
#     combine_groups:
#       - id: "course_info"
#         prefix: "Course information: "
#         columns: [...]
build_sdg_input <- function(guides_translated, sdg_cfg) {
  combine_groups <- sdg_cfg$combine_groups
  
  # ------------------------------------------------------------------
  # Fallback: no combine_groups -> behave like old "all_text" version
  # ------------------------------------------------------------------
  if (is.null(combine_groups) || !length(combine_groups)) {
    message(
      "build_sdg_input(): no combine_groups in config, ",
      "falling back to single 'all_text' section."
    )
    
    default_cols <- c(
      "course_name_en",
      "description_en",
      "contents_en",
      "competences_learning_results_en",
      "references_en"
    )
    
    cols_existing <- intersect(default_cols, names(guides_translated))
    if (!length(cols_existing)) {
      stop(
        "build_sdg_input(): no translatable columns found. ",
        "Expected one of: ", paste(default_cols, collapse = ", ")
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
        paste(pieces, collapse = " ||| ")
      }
    )
    
    sdg_input <- tibble::tibble(
      document_number = guides_translated$document_number,
      section         = "all_text",
      text            = text_vec
    ) |>
      dplyr::filter(!is.na(text) & text != "")
    
    return(sdg_input)
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
        "a non-empty 'id' field."
      )
    }
    
    if (!length(cols)) {
      warning(
        "build_sdg_input(): combine_groups entry '", id,
        "' has no columns; skipping."
      )
      next
    }
    
    cols_existing <- intersect(cols, names(guides_translated))
    missing_cols  <- setdiff(cols, cols_existing)
    
    if (length(missing_cols)) {
      warning(
        "build_sdg_input(): combine_groups '", id,
        "' refers to missing columns: ",
        paste(missing_cols, collapse = ", "),
        ". They will be ignored."
      )
    }
    
    if (!length(cols_existing)) {
      next
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
      "Check that the *_en columns exist and are not all empty."
    )
  }
  
  sdg_input
}


# Run text2sdg with configurable options
run_text2sdg_detection <- function(sdg_input, sdg_cfg) {
  method  <- sdg_cfg$method  %||% "systems"
  sdgs    <- sdg_cfg$sdgs    %||% 1:17
  verbose <- sdg_cfg$verbose %||% FALSE
  
  method <- tolower(method)
  
  if (identical(method, "systems")) {
    systems <- sdg_cfg$systems %||% c("Aurora", "Elsevier", "Auckland", "SIRIS")
    output  <- sdg_cfg$output  %||% "documents"
    
    # text2sdg::detect_sdg_systems() signature in your version:
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
    
  } else {
    stop("run_text2sdg_detection(): unknown method in config: ", method)
  }
  
  if (!nrow(hits)) {
    message("run_text2sdg_detection(): text2sdg returned 0 hits.")
    hits$document_number <- integer(0)
    hits$section         <- character(0)
    return(hits)
  }
  
  doc_index <- as.integer(hits$document)
  doc_map   <- sdg_input$document_number
  sec_map   <- sdg_input$section
  
  if (any(is.na(doc_index)) ||
      any(doc_index < 1L) ||
      any(doc_index > length(doc_map))) {
    stop("run_text2sdg_detection(): invalid 'document' indices in hits.")
  }
  
  hits %>%
    dplyr::mutate(
      document_number = doc_map[doc_index],
      section         = sec_map[doc_index]
    )
}


# Long summary: document_number x section x system x sdg
summarise_sdg_hits_long <- function(hits) {
  if (!nrow(hits)) {
    return(
      tibble::tibble(
        document_number = integer(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        n_hits          = integer(0),
        sdg_num         = character(0)
      )
    )
  }
  
  if (!"document_number" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'document_number' column is required.")
  }
  if (!"section" %in% names(hits)) {
    stop("summarise_sdg_hits_long(): 'section' column is required.")
  }
  
  hits %>%
    dplyr::group_by(
      document_number,
      section,
      system,
      sdg
    ) %>%
    dplyr::summarise(
      n_hits = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      sdg_num = stringr::str_extract(sdg, "\\d+")
    )
}

# Wide summary: 1 row per document_number, 1 col per (section, system, sdg)
summarise_sdg_hits_wide <- function(sdg_long, sdg_cfg) {
  if (!nrow(sdg_long)) {
    return(
      tibble::tibble(
        document_number = integer(0)
      )
    )
  }
  
  agg_mode <- sdg_cfg$aggregate$mode     %||% "counts"
  min_hits <- sdg_cfg$aggregate$min_hits %||% 1L
  
  sdg_long %>%
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
    ) %>%
    dplyr::select(
      document_number,
      colname,
      value
    ) %>%
    tidyr::pivot_wider(
      id_cols      = document_number,
      names_from   = colname,
      values_from  = value,
      values_fill  = 0
    )
}

# Attach SDG wide summary back to guides_translated
attach_sdg_to_guides <- function(guides_translated, sdg_hits_wide) {
  guides_translated %>%
    dplyr::left_join(sdg_hits_wide, by = "document_number")
}
