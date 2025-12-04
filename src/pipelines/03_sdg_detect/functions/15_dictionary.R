# File: src/pipelines/03_detect_sdg/functions/10_dictionary.R
# Purpose:
#   Build a query/term dictionary from text2sdg system queries.
#
# Notes:
#   - Extracts literal expressions inside quotes from text2sdg queries.
#   - Lowercases output.
#   - Expands wildcard (*) expressions using the curated expansions CSV
#     read by read_query_expansions().
#   - Does NOT filter single words by default (keep_single_word = TRUE).
#
# Dependencies:
#   - read_query_expansions() and expand_wildcard_term() must be available
#     (we keep them in 15_queries.R).

build_text2sdg_query_dictionary <- function(
    systems = c("Aurora","Elsevier","Auckland","SIRIS","SDGO","SDSN"),
    expansions_csv = NULL,
    keep_single_word = TRUE
) {
  systems <- unique(as.character(systems))
  
  # Load per-system query tables from {text2sdg}
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
    
    dplyr::mutate(x, system = sys, query = as.character(.data$query))
  })
  
  all_queries <- dplyr::bind_rows(tables)
  
  # Extract literals: pieces between double quotes "..."
  extract_literals <- function(q) {
    m <- stringr::str_match_all(q, "\"([^\"]+)\"")[[1]]
    if (nrow(m) == 0) return(character(0))
    m[, 2]
  }
  
  literals <- all_queries |>
    dplyr::mutate(expression = purrr::map(.data$query, extract_literals)) |>
    tidyr::unnest(expression) |>
    dplyr::mutate(
      expression = tolower(trimws(as.character(.data$expression))),
      n_words = stringr::str_count(.data$expression, "\\S+")
    ) |>
    dplyr::filter(!is.na(.data$expression), nzchar(.data$expression))
  
  # Expand wildcard expressions (only those containing *)
  expansions_tbl <- read_query_expansions(expansions_csv %||% "")
  if (nrow(expansions_tbl)) {
    literals <- literals |>
      dplyr::mutate(
        expression = purrr::map(.data$expression, function(e) {
          if (grepl("\\*", e, fixed = TRUE)) {
            expand_wildcard_term(e, expansions_tbl)
          } else {
            e
          }
        })
      ) |>
      tidyr::unnest(expression) |>
      dplyr::mutate(
        expression = tolower(trimws(as.character(.data$expression))),
        n_words = stringr::str_count(.data$expression, "\\S+")
      ) |>
      dplyr::filter(!is.na(.data$expression), nzchar(.data$expression))
  }
  
  if (!isTRUE(keep_single_word)) {
    literals <- literals |>
      dplyr::filter(.data$n_words > 1)
  }
  
  literals |>
    dplyr::select(system, expression) |>
    dplyr::distinct() |>
    dplyr::arrange(expression, system)
}
