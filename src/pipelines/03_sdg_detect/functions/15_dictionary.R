# File: src/pipelines/03_detect_sdg/functions/15_dictionary.R
# Purpose:
#   Build/load the expression dictionary used to:
#   - re-compose comma-split text2sdg features ("quality, of, life" -> "quality of life")
#   - flag in-dictionary vs out-of-dictionary (OOV) features
#
# Sources (priority):
#   1) dictionary_csv (curated; stable; can include single + multi-word)
#   2) fallback: build from text2sdg system queries + expansions_csv
#
# Notes:
#   - For recomposition rules we ONLY use multi-word expressions (n_words > 1).
#   - Dictionary can include single words too (useful for OOV checks / audits).

.read_expression_dictionary_csv <- function(path) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) {
    return(tibble::tibble(expression = character(0)))
  }
  
  x <- readr::read_csv(path, show_col_types = FALSE)
  nms <- tolower(names(x))
  names(x) <- nms
  
  col <- dplyr::case_when(
    "expression" %in% nms ~ "expression",
    "term"       %in% nms ~ "term",
    "feature"    %in% nms ~ "feature",
    TRUE ~ NA_character_
  )
  
  if (is.na(col)) {
    stop(
      "read_expression_dictionary_csv(): expected a column like `expression`/`term`/`feature` in: ", path,
      "\nFound columns: ", paste(names(x), collapse = ", "),
      call. = FALSE
    )
  }
  
  out <- x |>
    dplyr::transmute(expression = tolower(trimws(as.character(.data[[col]])))) |>
    dplyr::filter(!is.na(.data$expression), nzchar(.data$expression)) |>
    dplyr::distinct()
  
  out
}

# Build a dictionary from text2sdg query tables (quotes literals), and optionally expand wildcards using a curated CSV.
.build_dictionary_from_text2sdg <- function(
    systems = c("Aurora","Elsevier","Auckland","SIRIS","SDGO","SDSN"),
    expansions_csv = NULL,
    keep_single_word = TRUE
) {
  systems <- unique(as.character(systems))
  
  # load per-system query tables from {text2sdg}
  tables <- lapply(systems, function(sys) {
    obj <- switch(
      sys,
      "Aurora"   = "aurora_queries",
      "Elsevier" = "elsevier_queries",
      "Auckland" = "auckland_queries",
      "SIRIS"    = "siris_queries",
      "SDGO"     = "sdgo_queries",
      "SDSN"     = "sdsn_queries",
      stop("Unknown system: ", sys, call. = FALSE)
    )
    
    x <- get(obj, envir = asNamespace("text2sdg"))
    x <- tibble::as_tibble(x)
    
    if (!"query" %in% names(x)) {
      stop("text2sdg table ", obj, " has no `query` column.", call. = FALSE)
    }
    
    dplyr::mutate(x, query = as.character(.data$query))
  })
  
  all_queries <- dplyr::bind_rows(tables)
  
  extract_literals_vec <- function(q) {
    m <- stringr::str_match_all(q, "\"([^\"]+)\"")[[1]]
    if (nrow(m) == 0) return(character(0))
    m[, 2]
  }
  
  dict <- all_queries |>
    dplyr::mutate(.lits = purrr::map(.data$query, extract_literals_vec)) |>
    tidyr::unnest(.lits) |>
    dplyr::transmute(expression = tolower(trimws(as.character(.data$.lits)))) |>
    dplyr::filter(!is.na(.data$expression), nzchar(.data$expression)) |>
    dplyr::distinct()
  
  # Optional expansions file: you used a column `expansions` with comma-separated values.
  if (!is.null(expansions_csv) && nzchar(expansions_csv) && file.exists(expansions_csv)) {
    exp_df <- readr::read_csv(expansions_csv, show_col_types = FALSE)
    nms <- tolower(names(exp_df))
    names(exp_df) <- nms
    
    if (!("expansions" %in% nms)) {
      stop(
        ".build_dictionary_from_text2sdg(): expansions CSV must contain column `expansions`.\n",
        "File: ", expansions_csv,
        call. = FALSE
      )
    }
    
    expanded_long <- exp_df |>
      dplyr::filter(!is.na(.data$expansions)) |>
      tidyr::separate_rows(.data$expansions, sep = "\\s*,\\s*") |>
      dplyr::mutate(expansions = tolower(trimws(as.character(.data$expansions)))) |>
      dplyr::filter(nzchar(.data$expansions)) |>
      dplyr::distinct(expansions) |>
      dplyr::rename(expression = expansions) |>
      dplyr::distinct()
    
    # remove wildcard rows (with *) from base dict and append expansions
    dict <- dict |>
      dplyr::filter(!stringr::str_detect(.data$expression, "\\*")) |>
      dplyr::bind_rows(expanded_long) |>
      dplyr::distinct()
  }
  
  dict <- dict |>
    dplyr::mutate(n_words = stringr::str_count(.data$expression, "\\S+"))
  
  if (!isTRUE(keep_single_word)) {
    dict <- dict |>
      dplyr::filter(.data$n_words > 1)
  }
  
  dict |>
    dplyr::select(expression) |>
    dplyr::distinct() |>
    dplyr::arrange(.data$expression)
}

# Public entry point used by targets
build_text2sdg_query_dictionary <- function(
    systems = c("Aurora","Elsevier","Auckland","SIRIS","SDGO","SDSN"),
    expansions_csv = NULL,
    dictionary_csv = NULL,
    keep_single_word = TRUE
) {
  # 1) curated dictionary wins (stable)
  dict_csv <- .read_expression_dictionary_csv(dictionary_csv %||% "")
  if (nrow(dict_csv)) {
    dict_csv <- dict_csv |>
      dplyr::mutate(n_words = stringr::str_count(.data$expression, "\\S+"))
    
    if (!isTRUE(keep_single_word)) {
      dict_csv <- dict_csv |>
        dplyr::filter(.data$n_words > 1)
    }
    
    return(dict_csv |>
             dplyr::select(expression) |>
             dplyr::distinct() |>
             dplyr::arrange(.data$expression))
  }
  
  # 2) fallback: build from text2sdg + expansions
  .build_dictionary_from_text2sdg(
    systems = systems,
    expansions_csv = expansions_csv,
    keep_single_word = keep_single_word
  )
}

# Build substitution rules used to re-compose comma-split features:
# pattern (match-friendly): "quality, of, life"  replacement (original): "quality of life"
# Handles hyphens:
#   - dictionary "evidence-based medicine" => pattern "evidence, based, medicine"
# so it can match feature strings like "evidence, -, based, medicine" once normalized.
build_compound_substitution_rules <- function(dictionary_tbl) {
  if (is.null(dictionary_tbl) || !nrow(dictionary_tbl) || !"expression" %in% names(dictionary_tbl)) {
    return(list(patterns = character(0), replacements = character(0)))
  }
  
  # Keep the original expression (lowercased) as the replacement
  expr_raw <- tolower(trimws(as.character(dictionary_tbl$expression)))
  expr_raw <- unique(expr_raw[!is.na(expr_raw) & nzchar(expr_raw)])
  
  # Only multi-word expressions are meaningful for recomposition
  expr_raw <- expr_raw[stringr::str_count(expr_raw, "\\S+") > 1]
  if (!length(expr_raw)) {
    return(list(patterns = character(0), replacements = character(0)))
  }
  
  # Build a match-friendly version for patterns:
  # Treat intra-word hyphens as spaces so "evidence-based" matches "evidence, based"
  expr_pat <- gsub("(?<=\\w)-(?=\\w)", " ", expr_raw, perl = TRUE)
  expr_pat <- gsub("\\s+", " ", expr_pat, perl = TRUE)
  expr_pat <- trimws(expr_pat)
  
  # Longest first (avoid partial matches)
  ord <- order(stringr::str_count(expr_pat, "\\S+"), nchar(expr_pat), decreasing = TRUE)
  expr_raw <- expr_raw[ord]
  expr_pat <- expr_pat[ord]
  
  patterns <- gsub(" ", ", ", expr_pat, fixed = TRUE)
  replacements <- expr_raw
  
  list(patterns = patterns, replacements = replacements)
}
