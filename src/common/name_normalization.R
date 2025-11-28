# File: src/commons/name_normalization.R
# Person name normalization helpers shared across pipelines.
#
# PURPOSE:
#   - Standardise person names coming from scraped lists.
#   - Convert to title case while keeping common particles in lowercase.
#   - Support list-style cells separated by ";" or ";\n".
#
# NOTES:
#   - This module is intentionally small and focused.
#   - It should be applied after HTML -> text cleaning.
#   - Empty strings become NA_character_ to ease downstream translation skipping.

# -------------------------------------------------------------------
# Single-name normaliser (one string)
# -------------------------------------------------------------------

.simple_title_case_keep_particles_one <- function(x) {
  if (is.na(x) || !nzchar(x)) return(NA_character_)
  
  s <- stringi::stri_trans_totitle(stringi::stri_trans_tolower(x))
  s <- gsub("\\s+", " ", s, perl = TRUE)
  s <- trimws(s)
  
  particles <- c(
    "De","Del","Dels","Da","Das","Do","Dos","Di","Du","Des",
    "La","Las","Le","Les","Lo","Los",
    "Van","Von","Der","Den","Ten","Ter",
    "Al","El","Ibn","Bin","Bint",
    "I","Y","E"
  )
  
  for (p in particles) {
    pat <- paste0("(?<=\\s)", p, "(?=(\\s|,|\\.|;|:|\\)|\\]|\\}|$))")
    s <- gsub(pat, tolower(p), s, perl = TRUE)
  }
  
  # Apostrophe particles.
  s <- gsub("(?<=\\s)D'(?=\\p{L})", "d'", s, perl = TRUE)
  s <- gsub("(?<=\\s)L'(?=\\p{L})", "l'", s, perl = TRUE)
  
  # Multiword particles.
  s <- gsub("(?<=\\s)de\\s+la(?=(\\s|,|\\.|;|:|$))", "de la", s, perl = TRUE)
  s <- gsub("(?<=\\s)de\\s+les(?=(\\s|,|\\.|;|:|$))", "de les", s, perl = TRUE)
  s <- gsub("(?<=\\s)de\\s+los(?=(\\s|,|\\.|;|:|$))", "de los", s, perl = TRUE)
  s <- gsub("(?<=\\s)de\\s+las(?=(\\s|,|\\.|;|:|$))", "de las", s, perl = TRUE)
  
  s
}

# -------------------------------------------------------------------
# List-cell normalisers (one cell -> one string)
# -------------------------------------------------------------------

.normalize_name_list_cell_particles_one <- function(x, sep_out = ";\n") {
  if (is.na(x) || !nzchar(x)) return(NA_character_)
  
  parts <- unlist(strsplit(x, ";\\s*\\n|;\\s*", perl = TRUE))
  parts <- trimws(parts)
  parts <- parts[nzchar(parts)]
  if (!length(parts)) return(NA_character_)
  
  parts <- vapply(parts, .simple_title_case_keep_particles_one, character(1))
  paste(parts, collapse = sep_out)
}

# -------------------------------------------------------------------
# Public API (vectorised, safe in dplyr::mutate)
# -------------------------------------------------------------------

normalize_name_list_cell_particles <- function(x, sep_out = ";\n") {
  vapply(x, .normalize_name_list_cell_particles_one, character(1), sep_out = sep_out)
}
