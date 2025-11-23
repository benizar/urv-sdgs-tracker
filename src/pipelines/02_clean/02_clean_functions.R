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
# FUTURE BEHAVIOUR:
#   - Additional domain-specific cleaning rules can be added here
#     (for example, harmonising abbreviations, expanding URV-specific acronyms).
#   - This module can be extended with quality flags, such as:
#       * indicators for suspiciously short descriptions,
#       * missing key sections per course.
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
    # 0x09 = tab, 0x0A = LF, 0x0D = CR (CR will later be normalised in clean_basic_text)
    stringr::str_replace_all("[\\x00-\\x08\\x0B-\\x0C\\x0E-\\x1F\\x7F]", " ")
}


# -------------------------------------------------------------------
# Basic text cleaning helpers
# -------------------------------------------------------------------

# Light, generic cleaning for long text fields.
#
# Goals:
#   - Remove HTML tags and invisible characters.
#   - Normalise line breaks.
#   - Remove common bullet prefixes and trivial leading noise.
#   - Trim and squash whitespace.
#
# It intentionally does NOT:
#   - Lowercase everything.
#   - Remove punctuation aggressively.
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
#
# Goals:
#   - Strip HTML / invisible noise.
#   - Replace roman numerals at the end of the name with arabic digits,
#     using the same logic as the original script, including the special
#     case for "el VI" / "del VI".
#   - Replace straight apostrophes with your preferred curly form.
#   - Trim and squash spaces.
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
  
  # First remove HTML / invisible junk, then apply roman numeral logic
  name <- clean_html_and_invisible(x)
  
  # Replace all roman numerals except VI
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
    # Harmonise apostrophes
    stringr::str_replace_all("'", "’") %>%
    # Normalise spaces
    stringr::str_squish()
}


# Cleaning rules specific to references / bibliography.
#
# Goals:
#   - Strip HTML / invisible junk.
#   - Remove technical labels and prefixes from CRAI / URV catalogues
#     (for example: "(llibre)", "(revista)", "(bases de dades)", "accés al crai").
#   - Remove leading separators and bullet characters.
#   - Trim and normalise whitespace.
#
# It intentionally does NOT:
#   - Force lowercasing (we keep the original case as much as possible).
clean_references <- function(x) {
  patterns <- c(
    "accés al crai", "no disponible a la urv", ", més info", "més info",
    "diversos,", "diversos\\.",
    "\\(llibre\\)\\s*[,-/]*\\s*",
    "\\(revista\\)\\s*[,-/]*\\s*",
    "\\(altres\\)\\s*[,-/]*\\s*",
    "\\(lloc web\\)\\s*[,-/]*\\s*",
    "\\(bases de dades\\)\\s*[,-/]*\\s*",
    "\\(cap[ií]tol llibre\\)\\s*[,-/]*\\s*",
    "\\(audios\\)\\s*/\\s*",
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
# Main cleaning function operating on guides_index
# -------------------------------------------------------------------

# Clean the per-course index returned by 01_import (guides_index).
#
# INPUT:
#   - guides_index: data frame with one row per course_url, containing:
#       * structural metadata (faculty, degree, course details),
#       * concatenated long text fields
#         (description, contents, competences_learning_results, references),
#       * teaching staff information.
#
# OUTPUT:
#   - guides_clean: same rows and columns as guides_index, plus:
#       * course_name_clean
#       * description_clean
#       * contents_clean
#       * competences_learning_results_clean
#       * references_clean
#     and normalised numeric types for credits / year.
#
# NOTES:
#   - Original columns are preserved; nothing is overwritten.
#   - Cleaned columns are meant to be used as input for translation
#     and SDG classification.
clean_guides_index <- function(guides_index) {
  guides_index %>%
    dplyr::mutate(
      # Structural cleaning for course name
      course_name_clean = clean_course_name(course_name),
      
      # Light cleaning for long text fields (non-destructive)
      description_clean = clean_basic_text(description),
      contents_clean    = clean_basic_text(contents),
      competences_learning_results_clean =
        clean_basic_text(competences_learning_results),
      
      # Bibliography-specific cleaning
      references_clean  = clean_references(references),
      
      # Structural numeric fields
      credits = suppressWarnings(as.numeric(credits)),
      year    = suppressWarnings(as.integer(degree_year))
    )
}
