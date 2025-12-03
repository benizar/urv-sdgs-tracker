# File: src/common/text2sdg_queries_helpers.R
#
# Helpers to work with text2sdg query dictionaries and extracted "features".
#
# Responsibilities:
#   - Load text2sdg query dictionaries
#   - Extract quoted (literal) expressions from queries
#   - Expand wildcard (*) expressions via a curated CSV
#   - Augment text2sdg hits with query text + extracted literals
#   - Parse/normalize "features" returned by text2sdg (for wordclouds)
#
# Notes:
# - This file assumes a null-coalesce helper exists globally (e.g. %||% in common_utils.R).
# - This file is sourced from _targets.R via the src/common loader.

# ------------------------------------------------------------
# Features parsing
# ------------------------------------------------------------

parse_features <- function(x) {
  # text2sdg typically returns features as a string (often comma-separated).
  x <- stats::na.omit(as.character(x))
  x <- x[nzchar(x)]
  if (!length(x)) return(character(0))
  
  parts <- unlist(strsplit(x, "[,;|]"))
  parts <- stringr::str_trim(parts)
  parts <- parts[nzchar(parts)]
  parts <- tolower(parts)
  unique(parts)
}

# ------------------------------------------------------------
# Query literal extraction + wildcard expansions
# ------------------------------------------------------------

extract_quoted_literals <- function(query) {
  query <- as.character(query %||% "")
  if (!nzchar(query)) return(character(0))
  
  m <- stringr::str_match_all(query, '"([^"]+)"')[[1]]
  if (is.null(m) || nrow(m) == 0) return(character(0))
  
  out <- m[, 2]
  out <- tolower(stringr::str_trim(out))
  out <- out[nzchar(out)]
  unique(out)
}

read_queries_expansions <- function(path) {
  # Expected schema:
  #   - expression (wildcard literal, e.g. "child*")
  #   - expansions (comma-separated list)
  if (is.null(path) || !nzchar(path) || !file.exists(path)) {
    return(tibble::tibble(expression = character(0), expansions = character(0)))
  }
  
  df <- tryCatch(
    readr::read_csv(path, show_col_types = FALSE),
    error = function(e) NULL
  )
  if (is.null(df) || !nrow(df)) {
    return(tibble::tibble(expression = character(0), expansions = character(0)))
  }
  
  # Defensive normalization of column names.
  nms <- names(df)
  if (!"expression" %in% nms && "expr" %in% nms) names(df)[names(df) == "expr"] <- "expression"
  if (!"expansions" %in% nms && "expansion" %in% nms) names(df)[names(df) == "expansion"] <- "expansions"
  
  if (!all(c("expression", "expansions") %in% names(df))) {
    warning(
      "read_queries_expansions(): expansions CSV does not contain {expression, expansions}. Ignoring. Path: ",
      path
    )
    return(tibble::tibble(expression = character(0), expansions = character(0)))
  }
  
  df |>
    dplyr::transmute(
      expression = tolower(stringr::str_trim(as.character(expression))),
      expansions = tolower(stringr::str_trim(as.character(expansions)))
    ) |>
    dplyr::filter(nzchar(expression), nzchar(expansions)) |>
    dplyr::distinct(expression, expansions)
}

expand_wildcard_literals <- function(literals, expansions_tbl) {
  literals <- unique(stats::na.omit(as.character(literals)))
  literals <- literals[nzchar(literals)]
  if (!length(literals)) return(character(0))
  
  if (is.null(expansions_tbl) || !nrow(expansions_tbl)) {
    return(literals)
  }
  
  out <- character(0)
  for (lit in literals) {
    if (stringr::str_detect(lit, "\\*")) {
      row <- expansions_tbl[expansions_tbl$expression == lit, , drop = FALSE]
      if (nrow(row) >= 1) {
        exps <- unlist(strsplit(row$expansions[1], ","))
        exps <- tolower(stringr::str_trim(exps))
        exps <- exps[nzchar(exps)]
        out <- c(out, exps)
      } else {
        out <- c(out, lit)
      }
    } else {
      out <- c(out, lit)
    }
  }
  
  out <- unique(out)
  out <- out[nzchar(out)]
  out
}

# ------------------------------------------------------------
# Load + bind text2sdg query dictionaries
# ------------------------------------------------------------

load_text2sdg_queries <- function(systems = NULL) {
  # Supported text2sdg datasets we want to bind.
  datasets <- c(
    "aurora_queries",
    "auckland_queries",
    "elsevier_queries",
    "sdgo_queries",
    "sdsn_queries",
    "siris_queries"
  )
  
  out <- list()
  
  for (nm in datasets) {
    obj <- NULL
    
    # Try internal namespace access.
    obj <- tryCatch(get(nm, envir = asNamespace("text2sdg")), error = function(e) NULL)
    
    # Fallback: data() into local env.
    if (is.null(obj)) {
      ok <- tryCatch({
        data(list = nm, package = "text2sdg", envir = environment())
        TRUE
      }, error = function(e) FALSE)
      
      if (ok && exists(nm, envir = environment(), inherits = FALSE)) {
        obj <- get(nm, envir = environment(), inherits = FALSE)
      }
    }
    
    if (!is.null(obj)) out[[nm]] <- obj
  }
  
  if (!length(out)) {
    stop(
      "load_text2sdg_queries(): could not load query dictionaries from {text2sdg}.",
      call. = FALSE
    )
  }
  
  dict <- dplyr::bind_rows(out)
  
  # Minimal schema we rely on.
  needed <- c("system", "sdg", "query_id", "query")
  missing <- setdiff(needed, names(dict))
  if (length(missing)) {
    stop(
      "load_text2sdg_queries(): unexpected dictionary schema, missing: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }
  
  dict <- dict |>
    dplyr::mutate(
      system   = as.character(system),
      sdg      = as.character(sdg),
      query_id = suppressWarnings(as.integer(query_id)),
      query    = as.character(query)
    )
  
  if (!is.null(systems) && length(systems)) {
    dict <- dict |> dplyr::filter(system %in% as.character(systems))
  }
  
  dict
}

# ------------------------------------------------------------
# Build dictionary of quoted literals (per system + query_id)
# ------------------------------------------------------------

build_query_literals_dictionary <- function(sdg_cfg) {
  systems <- sdg_cfg$systems %||% NULL
  dict <- load_text2sdg_queries(systems = systems)
  
  expansions_path <- sdg_cfg$queries_expansions_csv %||% ""
  expansions_tbl  <- read_queries_expansions(expansions_path)
  
  keep_single_word <- isTRUE(sdg_cfg$keep_single_word_queries %||% FALSE)
  
  dict |>
    dplyr::transmute(
      system,
      query_id = suppressWarnings(as.integer(query_id)),
      sdg = as.character(sdg),
      query = as.character(query)
    ) |>
    dplyr::mutate(
      literals = purrr::map(query, extract_quoted_literals),
      literals = purrr::map(literals, expand_wildcard_literals, expansions_tbl = expansions_tbl),
      literals = purrr::map(literals, function(x) {
        x <- unique(stats::na.omit(as.character(x)))
        x <- x[nzchar(x)]
        if (!keep_single_word) {
          x <- x[stringr::str_count(x, "\\S+") > 1]
        }
        unique(x)
      })
    ) |>
    dplyr::select(system, query_id, sdg, query, literals)
}

# ------------------------------------------------------------
# Augment hits with query + literals if query_id exists
# ------------------------------------------------------------

augment_hits_with_query_info <- function(hits, sdg_cfg) {
  if (!nrow(hits)) return(hits)
  
  # If output="documents", some versions may not include query_id.
  if (!("system" %in% names(hits) && "query_id" %in% names(hits))) {
    hits$query <- NA_character_
    hits$query_literals <- list()
    hits$query_literals_n <- 0L
    hits$query_literals_collapsed <- ""
    return(hits)
  }
  
  dict_lit <- build_query_literals_dictionary(sdg_cfg)
  
  hits |>
    dplyr::mutate(
      system   = as.character(system),
      query_id = suppressWarnings(as.integer(query_id))
    ) |>
    dplyr::left_join(dict_lit, by = c("system", "query_id")) |>
    dplyr::mutate(
      query = dplyr::coalesce(query, NA_character_),
      query_literals = purrr::map(literals, function(x) unique(stats::na.omit(as.character(x)))),
      query_literals_n = purrr::map_int(query_literals, length),
      query_literals_collapsed = purrr::map_chr(query_literals, function(x) {
        x <- unique(stats::na.omit(as.character(x)))
        x <- x[nzchar(x)]
        if (!length(x)) "" else paste(sort(x), collapse = ", ")
      })
    ) |>
    dplyr::select(-literals)
}
