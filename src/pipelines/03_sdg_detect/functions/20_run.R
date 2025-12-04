# File: src/pipelines/03_sdg_detect/functions/20_run.R
# Purpose:
#   SDG detection (text2sdg) - execution wrapper.
#
# Contains:
#   - run_text2sdg_detection(): runs text2sdg either in "systems" mode
#     (per-system output) or "ensemble" mode, and maps hit indices back to:
#       document_number + section.
#
# Notes:
#   - `sdg_input` must be the output of build_sdg_input().
#   - We keep the raw output rows from text2sdg (including `features` and `query_id`
#     when output = "features") so downstream targets can derive summaries and
#     future feature-based exports without re-running detection.

run_text2sdg_detection <- function(sdg_input, sdg_cfg) {
  method  <- tolower(sdg_cfg$method %||% "systems")
  sdgs    <- sdg_cfg$sdgs %||% 1:17
  verbose <- sdg_cfg$verbose %||% FALSE
  output  <- sdg_cfg$output %||% "features"
  
  if (!nrow(sdg_input)) {
    return(tibble::tibble(
      document_number = character(0),
      section         = character(0),
      system          = character(0),
      sdg             = character(0),
      document        = integer(0)
    ))
  }
  
  if (!all(c("document_number", "section", "text") %in% names(sdg_input))) {
    stop(
      "run_text2sdg_detection(): sdg_input must contain columns: document_number, section, text.",
      call. = FALSE
    )
  }
  
  docs <- sdg_input$text
  
  if (identical(method, "systems")) {
    systems <- sdg_cfg$systems %||% c("Aurora","Elsevier","Auckland","SIRIS","SDGO","SDSN")
    
    hits <- text2sdg::detect_sdg_systems(
      docs,
      sdgs    = sdgs,
      systems = systems,
      output  = output,
      verbose = verbose
    )
  } else if (identical(method, "ensemble")) {
    hits <- text2sdg::detect_sdg(
      docs,
      sdgs    = sdgs,
      output  = output,
      verbose = verbose
    )
    
    if (!"system" %in% names(hits)) hits$system <- "ensemble"
  } else {
    stop("run_text2sdg_detection(): invalid method: ", method, call. = FALSE)
  }
  
  # Ensure a stable empty return with expected columns, but do not drop any
  # additional columns that text2sdg may return (e.g. features, query_id).
  if (!nrow(hits)) {
    message("run_text2sdg_detection(): text2sdg returned 0 hits.")
    
    # If empty, make sure downstream code can rely on these columns existing.
    hits$document_number <- character(0)
    hits$section         <- character(0)
    
    if (!"system" %in% names(hits)) hits$system <- character(0)
    if (!"sdg" %in% names(hits))    hits$sdg    <- character(0)
    
    return(hits)
  }
  
  if (!"document" %in% names(hits)) {
    stop("run_text2sdg_detection(): text2sdg output does not include 'document' column.", call. = FALSE)
  }
  
  doc_index <- as.integer(hits$document)
  doc_map   <- sdg_input$document_number
  sec_map   <- sdg_input$section
  
  if (any(is.na(doc_index)) || any(doc_index < 1L) || any(doc_index > length(doc_map))) {
    stop("run_text2sdg_detection(): invalid 'document' indices in hits.", call. = FALSE)
  }
  
  hits |>
    dplyr::mutate(
      document_number = doc_map[doc_index],
      section         = sec_map[doc_index],
      sdg             = as.character(sdg),
      system          = as.character(system)
    )
}
