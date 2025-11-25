# File: src/pipelines/02_clean/02_clean_functions.R
# Cleaning functions for the URV SDGs tracker guides dataset.
#
# CURRENT BEHAVIOUR:
#   - Takes the per-course index produced by 01_import (guides_index).
#   - Applies light, mostly structural cleaning to key text and numeric fields:
#       * Normalises whitespace and line breaks.
#       * Unifies bullets and removes trivial noise.
#       * Normalises course names (roman numerals, apostrophes, spaces).
#       * Cleans CRAI-specific labels from references.
#       * Casts credits / year to numeric types.
#   - Stores cleaned versions in new *_clean columns (non-destructive).
#
# IMPORTANT:
#   - Original columns (course_name, description, contents, etc.) are preserved.
#   - Cleaned columns are intended as the main input for translation and SDG detection.


# -------------------------------------------------------------------
# Helper: strip HTML tags and invisible / control characters
# -------------------------------------------------------------------

clean_html_and_invisible <- function(x) {
  # Work safely with empty vectors and non-character columns
  if (is.null(x)) {
    return(x)
  }
  
  x <- as.character(x)
  
  x %>%
    # Remove zero-width and BOM characters
    stringr::str_replace_all("[\u200B-\u200D\uFEFF]", "") %>%
    
    # Normalise non-breaking spaces to regular spaces
    stringr::str_replace_all("\u00A0", " ") %>%          # literal NBSP
    stringr::str_replace_all("&nbsp;", " ") %>%
    
    # Decode a few common HTML entities (best-effort, not exhaustive)
    stringr::str_replace_all("&amp;", "&") %>%
    stringr::str_replace_all("&lt;", "<") %>%
    stringr::str_replace_all("&gt;", ">") %>%
    stringr::str_replace_all("&quot;", "\"") %>%
    stringr::str_replace_all("&apos;", "'") %>%
    
    # Remove common HTML tags (p, br, b, i, strong, em, lists, etc.)
    # Case-insensitive; replace with a space to avoid accidental concatenation.
    stringr::str_replace_all("(?i)</?(p|br|ul|ol|li|b|strong|i|em)[^>]*>", " ") %>%
    
    # Fallback: remove any remaining generic HTML tags
    stringr::str_replace_all("<[^>]+>", " ") %>%
    
    # Remove control characters except tab/newline (keep structure if any)
    # 0x09 = tab, 0x0A = LF, 0x0D = CR
    stringr::str_replace_all("[\\x00-\\x08\\x0B-\\x0C\\x0E-\\x1F\\x7F]", " ")
}


# -------------------------------------------------------------------
# Basic text cleaning helpers
# -------------------------------------------------------------------

# Light, generic cleaning for long text fields.
clean_basic_text <- function(x) {
  x %>%
    clean_html_and_invisible() %>%
    # Normalise line breaks to "\n"
    stringr::str_replace_all("\r\n", "\n") %>%
    stringr::str_replace_all("\r", "\n") %>%
    # Remove common bullet-like prefixes at the beginning of the string
    stringr::str_replace_all("^[[:space:]]*[-–—•]+[[:space:]]*", "") %>%
    # Remove leading "NA" tokens that are artefacts from scraping
    stringr::str_replace_all("^(NA|Na|na)[[:space:]]*", "") %>%
    # Final trim and space squishing
    stringr::str_trim() %>%
    stringr::str_squish()
}

# Normalise course names.
clean_course_name <- function(x) {
  replacements <- c(
    " VIII$" = " 8",
    " VII$"  = " 7",
    " V$"    = " 5",
    " IV$"   = " 4",
    " III$"  = " 3",
    " II$"   = " 2",
    " I$"    = " 1"
  )
  
  name <- clean_html_and_invisible(x)
  
  for (pattern in names(replacements)) {
    name <- stringr::str_replace(name, pattern, replacements[[pattern]])
  }
  
  # Special case for VI:
  # Do not replace when the name ends with " el VI" or " del VI".
  name <- dplyr::if_else(
    stringr::str_detect(name, "(?i)( el VI| del VI)$"),
    name,
    stringr::str_replace(name, " VI$", " 6")
  )
  
  name %>%
    stringr::str_replace_all("'", "’") %>%
    stringr::str_squish()
}

# Cleaning rules specific to references / bibliography.
clean_references <- function(x) {
  patterns <- c(
    "Accés al CRAI", "No disponible a la URV", ", Més info",
    "diversos,", "diversos\\.",
    "\\(Llibre\\)\\s*[,-/]*\\s*",
    "\\(Revista\\)\\s*[,-/]*\\s*",
    "\\(Altres\\)\\s*[,-/]*\\s*",
    "\\(Lloc web\\)\\s*[,-/]*\\s*",
    "\\(Bases de dades\\)\\s*[,-/]*\\s*",
    "\\(Cap[ií]tol llibre\\)\\s*[,-/]*\\s*",
    "\\(Audios\\)\\s*/\\s*",
    "^/\\s*", "^:\\s*", "^,\\s*", "^•\\s*", "^-\\s*", "^\\.\\s*"
  )
  
  out <- clean_html_and_invisible(x)
  
  for (p in patterns) {
    out <- stringr::str_replace_all(out, p, "")
  }
  
  out %>%
    stringr::str_trim() %>%
    stringr::str_squish()
}

# -------------------------------------------------------------------
# Competences / learning results: hierarchical + code cleanup
# -------------------------------------------------------------------

# Detect header tokens that appear in some scraped outputs, e.g.:
# "Tipus A Codi Resultats d'aprenentatge"
is_comp_header_item <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  xi <- stringr::str_squish(x)
  
  stringr::str_detect(
    stringr::str_to_lower(xi),
    "^tipus\\s+[a-z]\\s+codi\\s+resultats\\s+d['’]?aprenentatge$"
  ) ||
    stringr::str_detect(stringr::str_to_lower(xi), "^tipus\\s+[a-z]\\s+codi$") ||
    stringr::str_detect(stringr::str_to_lower(xi), "^codi\\s+resultats\\s+d['’]?aprenentatge$")
}

# Remove leading code prefixes, keeping only the descriptive text.
strip_comp_code_prefix <- function(item) {
  item <- stringr::str_squish(item)
  if (is.na(item) || !nzchar(item)) return(NA_character_)
  if (is_comp_header_item(item)) return(NA_character_)
  
  out <- stringr::str_replace(
    item,
    "^[A-Za-z]{1,8}\\d{1,4}(?:\\.\\d+){0,3}\\s*[-–—]\\s*",
    ""
  )
  
  out <- stringr::str_squish(out)
  if (!nzchar(out)) NA_character_ else out
}

# Expand one scraped item into {parent, child} when appropriate.
# This guarantees that when a line is "PARENT - parent text - CHILD-child text",
# we keep ONE instance of the parent and ONE instance of the child.
parse_comp_item <- function(item) {
  item <- stringr::str_squish(item)
  if (is.na(item) || !nzchar(item)) return(character(0))
  if (is_comp_header_item(item)) return(character(0))
  
  # Split hierarchy boundaries only on dashes with SPACES around them.
  # This preserves "SE11.1-Aplicar" intact.
  parts <- stringr::str_split(item, "\\s+[-–—]\\s+")[[1]]
  parts <- stringr::str_squish(parts)
  parts <- parts[nzchar(parts)]
  
  is_parent_code <- function(z) stringr::str_detect(z, "^[A-Za-z]{1,8}\\d{1,4}$")
  
  # Parent-child (3+ parts): "K2 - Parent text - K2.4-Child text"
  if (length(parts) >= 3 && is_parent_code(parts[1])) {
    parent <- paste0(parts[1], " - ", parts[2])
    
    child_raw <- paste(parts[3:length(parts)], collapse = " - ")
    child_raw <- stringr::str_squish(child_raw)
    
    # Normalise the child to "CODE-text" (no spaces around the internal dash)
    m <- stringr::str_match(
      child_raw,
      "^([A-Za-z]{1,8}\\d{1,4}(?:\\.\\d+)+)\\s*[-–—]\\s*(.+)$"
    )
    if (!is.na(m[1, 1])) {
      child <- paste0(m[1, 2], "-", stringr::str_squish(m[1, 3]))
      return(c(parent, child))
    }
    
    return(parent)
  }
  
  # Parent-only (2 parts): "K2 - Parent text"
  if (length(parts) == 2 && is_parent_code(parts[1])) {
    return(paste0(parts[1], " - ", parts[2]))
  }
  
  # Child-only: "K2.4-Child text"
  m2 <- stringr::str_match(
    item,
    "^([A-Za-z]{1,8}\\d{1,4}(?:\\.\\d+)+)\\s*[-–—]\\s*(.+)$"
  )
  if (!is.na(m2[1, 1])) {
    return(paste0(m2[1, 2], "-", stringr::str_squish(m2[1, 3])))
  }
  
  # Fallback: keep as-is
  item
}

# Vectorised cleaner:
#   1) clean_basic_text() for whitespace/HTML
#   2) split into ";" items
#   3) expand hierarchical items into parent + child (so parent isn't lost)
#   4) de-duplicate while keeping first-seen order
#   5) drop headers
#   6) strip code prefixes
clean_competences_learning_results <- function(x) {
  x <- clean_basic_text(x)
  
  vapply(x, function(xi) {
    if (is.na(xi) || !nzchar(stringr::str_squish(xi))) return(NA_character_)
    
    items <- stringr::str_split(xi, "\\s*;\\s*")[[1]]
    items <- items[!is.na(items)]
    items <- stringr::str_squish(items)
    items <- items[nzchar(items)]
    if (!length(items)) return(NA_character_)
    
    expanded <- unlist(lapply(items, parse_comp_item), use.names = FALSE)
    expanded <- expanded[!is.na(expanded)]
    expanded <- stringr::str_squish(expanded)
    expanded <- expanded[nzchar(expanded)]
    if (!length(expanded)) return(NA_character_)
    
    # De-dupe while preserving first-seen order (per course)
    expanded <- expanded[!duplicated(expanded)]
    
    cleaned <- vapply(expanded, strip_comp_code_prefix, character(1))
    cleaned <- cleaned[!is.na(cleaned)]
    cleaned <- stringr::str_squish(cleaned)
    cleaned <- cleaned[nzchar(cleaned)]
    
    # De-dupe again after code stripping
    cleaned <- cleaned[!duplicated(cleaned)]
    
    if (!length(cleaned)) return(NA_character_)
    paste(cleaned, collapse = " ; ")
  }, character(1))
}

# -------------------------------------------------------------------
# Main cleaning function operating on guides_index
# -------------------------------------------------------------------

clean_guides_index <- function(guides_index) {
  guides_index %>%
    dplyr::mutate(
      # Structural cleaning for course name
      course_name_clean = clean_course_name(course_name),
      
      # Light cleaning for long text fields (non-destructive)
      description_clean = clean_basic_text(description),
      contents_clean    = clean_basic_text(contents),
      
      # Competences / learning results (specialised)
      competences_learning_results_clean =
        clean_competences_learning_results(competences_learning_results),
      
      # Bibliography-specific cleaning
      references_clean  = clean_references(references),
      
      # Structural numeric fields
      credits = suppressWarnings(as.numeric(credits)),
      year    = suppressWarnings(as.integer(degree_year))
    )
}
