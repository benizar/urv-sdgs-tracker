# File: src/pipelines/03_sdg_detect/functions/15_queries.R
#
# Purpose:
#   Helpers to work with text2sdg query dictionaries and extracted "features".
#
# Responsibilities:
#   - Load curated wildcard expansions (CSV) with flexible column names
#   - Normalize / parse text2sdg `features` strings into one-term-per-row tables
#   - Optionally flag whether a feature is present in the query dictionary

# -------------------------------------------------------------------
# Read wildcard expansions CSV (curated)
# Supports:
#   - columns: expression/query/pattern + expansion/expanded/term OR expansions (comma-separated list)
# -------------------------------------------------------------------
read_query_expansions <- function(csv_path) {
  if (is.null(csv_path) || !nzchar(csv_path)) {
    return(tibble::tibble(query = character(0), expansion = character(0)))
  }
  
  if (!file.exists(csv_path)) {
    message("read_query_expansions(): expansions CSV not found, skipping expansion: ", csv_path)
    return(tibble::tibble(query = character(0), expansion = character(0)))
  }
  
  x <- readr::read_csv(csv_path, show_col_types = FALSE)
  
  nms <- tolower(names(x))
  names(x) <- nms
  
  q_col <- dplyr::case_when(
    "query"      %in% nms ~ "query",
    "expression" %in% nms ~ "expression",
    "expr"       %in% nms ~ "expr",
    "pattern"    %in% nms ~ "pattern",
    "wildcard"   %in% nms ~ "wildcard",
    TRUE ~ NA_character_
  )
  
  e_col <- dplyr::case_when(
    "expansion"  %in% nms ~ "expansion",
    "expanded"   %in% nms ~ "expanded",
    "term"       %in% nms ~ "term",
    "value"      %in% nms ~ "value",
    "expansions" %in% nms ~ "expansions",  # your file often uses this
    TRUE ~ NA_character_
  )
  
  if (is.na(q_col) || is.na(e_col)) {
    stop(
      "read_query_expansions(): CSV must contain columns for query/expression/pattern and expansion/term/expansions.\n",
      "Found columns: ", paste(names(x), collapse = ", "),
      call. = FALSE
    )
  }
  
  out <- x |>
    dplyr::transmute(
      query_raw = trimws(as.character(.data[[q_col]])),
      exp_raw   = as.character(.data[[e_col]])
    ) |>
    dplyr::filter(!is.na(query_raw), query_raw != "", !is.na(exp_raw), trimws(exp_raw) != "")
  
  # If we have a single "expansions" cell with comma-separated expansions, split to rows.
  if (identical(e_col, "expansions")) {
    out <- out |>
      tidyr::separate_rows(exp_raw, sep = "\\s*,\\s*") |>
      dplyr::mutate(exp_raw = trimws(exp_raw))
  }
  
  out |>
    dplyr::transmute(
      query     = query_raw,
      expansion = exp_raw
    ) |>
    dplyr::filter(!is.na(expansion), expansion != "") |>
    dplyr::distinct()
}

# -------------------------------------------------------------------
# Normalization helpers for feature strings
# -------------------------------------------------------------------

# Normalize a candidate feature phrase:
# - lowercase
# - convert commas/punctuation to spaces
# - remove duplicate tokens (case-insensitive), preserving order
# - collapse whitespace
.normalize_feature_phrase <- function(x) {
  if (is.null(x) || is.na(x) || !nzchar(trimws(x))) return(NA_character_)
  
  s <- tolower(trimws(as.character(x)))
  
  # Treat commas as separators; also strip light punctuation to avoid "urban)" etc.
  s <- gsub("[,;/|]+", " ", s, perl = TRUE)
  s <- gsub("[()\\[\\]{}\"']", " ", s, perl = TRUE)
  
  # Collapse multiple spaces
  s <- gsub("\\s+", " ", s, perl = TRUE)
  s <- trimws(s)
  if (!nzchar(s)) return(NA_character_)
  
  # Tokenize and de-duplicate tokens inside the phrase
  toks <- unlist(strsplit(s, "\\s+", perl = TRUE), use.names = FALSE)
  toks <- toks[nzchar(toks)]
  if (!length(toks)) return(NA_character_)
  
  # Deduplicate while preserving first occurrence
  toks <- toks[!duplicated(toks)]
  s2 <- paste(toks, collapse = " ")
  if (!nzchar(s2)) return(NA_character_)
  
  s2
}

# Split text2sdg features field into items.
# Input looks like: "Urban, Planning" or "work, work, work" etc.
.parse_features_items <- function(x) {
  if (is.null(x) || is.na(x) || !nzchar(trimws(x))) return(character(0))
  
  items <- unlist(strsplit(x, "\\s*[|;]\\s*|\\r?\\n+", perl = TRUE), use.names = FALSE)
  items <- trimws(items)
  items[nzchar(items)]
}


# Expand one wildcard query like "child*" -> multiple expansions (if present)
expand_wildcard_term <- function(term, expansions_tbl) {
  if (is.null(term) || is.na(term) || !nzchar(trimws(term))) return(character(0))
  t <- trimws(term)
  
  if (!grepl("\\*", t, fixed = TRUE)) return(t)
  
  if (is.null(expansions_tbl) || !nrow(expansions_tbl)) return(t)
  
  ex <- expansions_tbl$expansion[expansions_tbl$query == t]
  ex <- unique(trimws(ex))
  ex <- ex[nzchar(ex)]
  
  if (!length(ex)) t else ex
}

# Extract a character vector of dictionary terms from a dictionary object (flexible)
.get_dictionary_terms <- function(dict) {
  if (is.null(dict) || !nrow(dict)) return(character(0))
  
  nms <- names(dict)
  col <- dplyr::case_when(
    "expression" %in% nms ~ "expression",
    "term"       %in% nms ~ "term",
    "feature"    %in% nms ~ "feature",
    "query"      %in% nms ~ "query",
    TRUE ~ NA_character_
  )
  
  if (is.na(col)) return(character(0))
  
  terms <- tolower(trimws(as.character(dict[[col]])))
  terms <- terms[!is.na(terms) & nzchar(terms)]
  unique(terms)
}

# -------------------------------------------------------------------
# Public: Build a long table of normalized(+expanded) features from hits
# -------------------------------------------------------------------
extract_features_long <- function(hits, sdg_cfg, sdg_query_dictionary = NULL) {
  if (!nrow(hits) || !"features" %in% names(hits)) {
    return(tibble::tibble(
      document_number = character(0),
      section         = character(0),
      system          = character(0),
      sdg             = character(0),
      query_id        = integer(0),
      feature         = character(0),
      feature_raw     = character(0),
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
  
  # Optional expansions (mainly useful if any feature ever contains '*')
  expansions_path <- sdg_cfg$queries_expansions_csv %||% ""
  expansions_tbl  <- read_query_expansions(expansions_path)
  
  # Dictionary terms (optional flag; does not filter by default)
  dict_terms <- .get_dictionary_terms(sdg_query_dictionary)
  
  # Some text2sdg outputs include query_id; if missing, keep NA_integer_
  if (!"query_id" %in% names(hits)) hits$query_id <- NA_integer_
  
  tmp <- hits |>
    dplyr::mutate(
      feature_raw = as.character(features),
      .items      = lapply(feature_raw, .parse_features_items)
    ) |>
    tidyr::unnest(.items, keep_empty = FALSE) |>
    dplyr::transmute(
      document_number,
      section,
      system = as.character(system),
      sdg    = as.character(sdg),
      query_id = suppressWarnings(as.integer(query_id)),
      feature_raw,
      feature_item = as.character(.items)
    ) |>
    dplyr::filter(!is.na(feature_item), nzchar(trimws(feature_item)))
  
  if (!nrow(tmp)) {
    return(tibble::tibble(
      document_number = character(0),
      section         = character(0),
      system          = character(0),
      sdg             = character(0),
      query_id        = integer(0),
      feature         = character(0),
      feature_raw     = character(0),
      in_dictionary   = logical(0)
    ))
  }
  
  # Normalize (lowercase + dedupe tokens) then optional wildcard expansion
  expanded <- tmp |>
    dplyr::mutate(
      feature_norm = vapply(feature_item, .normalize_feature_phrase, character(1)),
      feature_norm = dplyr::na_if(feature_norm, "")
    ) |>
    dplyr::filter(!is.na(feature_norm), feature_norm != "") |>
    dplyr::mutate(
      feature = lapply(feature_norm, expand_wildcard_term, expansions_tbl = expansions_tbl)
    ) |>
    tidyr::unnest(feature, keep_empty = FALSE) |>
    dplyr::mutate(
      feature = vapply(feature, .normalize_feature_phrase, character(1)),
      feature = dplyr::na_if(feature, "")
    ) |>
    dplyr::filter(!is.na(feature), feature != "") |>
    dplyr::distinct(document_number, section, system, sdg, query_id, feature, feature_raw)
  
  
  if (length(dict_terms)) {
    expanded <- expanded |>
      dplyr::mutate(in_dictionary = feature %in% dict_terms)
  } else {
    expanded <- expanded |>
      dplyr::mutate(in_dictionary = NA)
  }
  
  expanded |>
    dplyr::select(document_number, section, system, sdg, query_id, feature, feature_raw, in_dictionary)
}
