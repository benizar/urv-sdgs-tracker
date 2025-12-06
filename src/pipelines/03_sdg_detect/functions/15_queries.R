# File: src/pipelines/03_sdg_detect/functions/15_queries.R
# Purpose:
#   Post-process text2sdg hits to obtain one feature per row, with “compound”
#   expressions re-composed using fixed string substitutions (your original approach).
#
# Key idea:
#   - Build patterns: "quality of life" -> "quality, of, life"
#   - Apply Reduce(gsub(..., fixed=TRUE)) over raw `features`
#   - Split by comma to get final features (compounds preserved, unknowns stay as singles)
#
# Notes:
#   - This code does NOT try to "guess" compounds outside the dictionary.
#   - It only recomposes compounds that exist in `compound_rules` / dictionary.

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

.normalize_features_commas <- function(x) {
  # Vectorised. Input: character vector. Output: character vector.
  if (is.null(x)) return(rep(NA_character_, 0))
  
  x <- as.character(x)
  x <- tolower(x)
  
  # --- NEW: normalize hyphens so compounds can match ---
  # 1) if text2sdg produced "-"" as its own token between commas, drop it:
  #    "evidence, -, based, medicine" -> "evidence, based, medicine"
  x <- gsub("\\s*,\\s*-\\s*,\\s*", ", ", x, perl = TRUE)
  
  # 2) if a hyphen is inside a word, treat it like a space for matching:
  #    "evidence-based medicine" -> "evidence based medicine"
  x <- gsub("(?<=\\w)-(?=\\w)", " ", x, perl = TRUE)
  
  # Normalise separators that may appear between feature chunks
  x <- gsub("\\r?\\n+", " | ", x, perl = TRUE)
  x <- gsub("\\s*[;|]+\\s*", " | ", x, perl = TRUE)
  
  # Now normalise comma spacing inside each chunk so fixed patterns match
  chunks_list <- strsplit(x, "\\s*\\|\\s*", perl = TRUE)
  
  out <- vapply(chunks_list, function(chunks) {
    chunks <- chunks[nzchar(trimws(chunks))]
    if (!length(chunks)) return("")
    
    chunks2 <- vapply(chunks, function(one) {
      toks <- unlist(strsplit(one, "\\s*,\\s*", perl = TRUE), use.names = FALSE)
      toks <- trimws(toks)
      toks <- toks[nzchar(toks)]
      if (!length(toks)) return("")
      paste(toks, collapse = ", ")
    }, character(1))
    
    chunks2 <- chunks2[nzchar(chunks2)]
    paste(chunks2, collapse = " | ")
  }, character(1))
  
  out[out == ""] <- NA_character_
  out
}

.build_compound_rules_from_dictionary <- function(compound_dictionary) {
  # compound_dictionary: character vector of expressions (ideally multi-word).
  if (is.null(compound_dictionary) || !length(compound_dictionary)) {
    return(list(patterns = character(0), replacements = character(0)))
  }
  
  expr <- tolower(trimws(as.character(compound_dictionary)))
  expr <- expr[!is.na(expr) & nzchar(expr)]
  expr <- unique(expr)
  
  # Keep multi-word only (compounds)
  expr <- expr[stringr::str_count(expr, "\\S+") > 1]
  if (!length(expr)) {
    return(list(patterns = character(0), replacements = character(0)))
  }
  
  # Longest first to avoid partial matching shadowing longer ones
  ord <- order(stringr::str_count(expr, "\\S+"), nchar(expr), decreasing = TRUE)
  expr <- expr[ord]
  
  list(
    patterns     = gsub(" ", ", ", expr, fixed = TRUE),
    replacements = expr
  )
}

.apply_compound_substitutions <- function(features_vec, rules) {
  if (is.null(features_vec) || !length(features_vec)) return(features_vec)
  if (is.null(rules) || is.null(rules$patterns) || !length(rules$patterns)) return(features_vec)
  
  Reduce(
    function(acc, i) gsub(rules$patterns[i], rules$replacements[i], acc, fixed = TRUE),
    seq_along(rules$patterns),
    init = features_vec
  )
}

.split_features_final <- function(features_fixed) {
  # Input: character vector. Output: list of character vectors (one per row).
  if (is.null(features_fixed)) return(list())
  
  chunks_list <- strsplit(features_fixed, "\\s*\\|\\s*", perl = TRUE)
  
  lapply(chunks_list, function(chunks) {
    if (!length(chunks)) return(character(0))
    
    toks <- unlist(strsplit(chunks, "\\s*,\\s*", perl = TRUE), use.names = FALSE)
    toks <- tolower(trimws(toks))
    toks <- toks[nzchar(toks)]
    if (!length(toks)) return(character(0))
    
    sort(unique(toks))
  })
}

.get_dictionary_terms <- function(sdg_query_dictionary) {
  if (is.null(sdg_query_dictionary) || !nrow(sdg_query_dictionary)) return(character(0))
  
  col <- dplyr::case_when(
    "expression" %in% names(sdg_query_dictionary) ~ "expression",
    "term"       %in% names(sdg_query_dictionary) ~ "term",
    TRUE ~ names(sdg_query_dictionary)[1]
  )
  
  terms <- tolower(trimws(as.character(sdg_query_dictionary[[col]])))
  terms <- terms[!is.na(terms) & nzchar(terms)]
  unique(terms)
}

# -------------------------------------------------------------------
# Public: one feature per row (after compound recomposition)
# -------------------------------------------------------------------

extract_features_long <- function(
    hits,
    sdg_cfg,
    compound_rules = NULL,
    compound_dictionary = NULL,
    sdg_query_dictionary = NULL,
    ...
) {
  if (!nrow(hits) || !"features" %in% names(hits)) {
    return(tibble::tibble(
      document_number = character(0),
      section         = character(0),
      system          = character(0),
      sdg             = character(0),
      query_id        = integer(0),
      feature         = character(0),
      feature_raw     = character(0),
      feature_fixed   = character(0),
      in_dictionary   = logical(0)
    ))
  }
  
  needed <- c("document_number", "section", "system", "sdg")
  if (!all(needed %in% names(hits))) {
    stop(
      "extract_features_long(): hits must contain columns: ",
      paste(needed, collapse = ", "),
      call. = FALSE
    )
  }
  
  if (!"query_id" %in% names(hits)) hits$query_id <- NA_integer_
  
  # Determine compound rules:
  # - If compound_rules is provided, use it.
  # - Else if compound_dictionary is provided, build rules from it.
  if (is.null(compound_rules)) {
    compound_rules <- .build_compound_rules_from_dictionary(compound_dictionary)
  } else {
    # ensure expected structure
    if (is.null(compound_rules$patterns) || is.null(compound_rules$replacements)) {
      stop(
        "extract_features_long(): compound_rules must be a list with `patterns` and `replacements`.",
        call. = FALSE
      )
    }
  }
  
  dict_terms <- .get_dictionary_terms(sdg_query_dictionary)
  
  df <- tibble::tibble(
    document_number = as.character(hits$document_number),
    section         = as.character(hits$section),
    system          = as.character(hits$system),
    sdg             = as.character(hits$sdg),
    query_id        = suppressWarnings(as.integer(hits$query_id)),
    feature_raw     = as.character(hits$features)
  )
  
  # 1) Normalise so fixed patterns match
  df$feature_norm <- .normalize_features_commas(df$feature_raw)
  
  # 2) Apply compound recomposition substitutions
  df$feature_fixed <- .apply_compound_substitutions(df$feature_norm, compound_rules)
  
  # 3) Split to final features (comma tokens), keeping compounds intact
  feat_list <- .split_features_final(df$feature_fixed)
  
  out <- df |>
    dplyr::mutate(.feat = feat_list) |>
    tidyr::unnest(.feat, keep_empty = FALSE) |>
    dplyr::rename(feature = .feat) |>
    dplyr::mutate(feature = tolower(trimws(as.character(.data$feature)))) |>
    dplyr::filter(!is.na(.data$feature), nzchar(.data$feature)) |>
    dplyr::distinct(
      .data$document_number, .data$section, .data$system, .data$sdg, .data$query_id,
      .data$feature, .data$feature_raw, .data$feature_fixed
    ) |>
    dplyr::mutate(
      in_dictionary = if (length(dict_terms)) (.data$feature %in% dict_terms) else NA
    ) |>
    dplyr::select(
      document_number, section, system, sdg, query_id,
      feature, feature_raw, feature_fixed, in_dictionary
    )
  
  out
}
