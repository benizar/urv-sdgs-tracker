# File: src/commons/text_cleaning.R
# Text cleaning helpers shared across pipelines.
#
# PURPOSE:
#   - Provide a robust HTML -> plain text extractor for scraped fragments.
#   - Remove common HTML blocks (<style>, <script>, comments).
#   - Convert <br> into line breaks and parse table rows/cells when present.
#   - Drop known label fragments (e.g., "DESCRIPCIÓ GENERAL DE L'ASSIGNATURA:").
#   - Sanitize strange symbols and remove bullet markers at line starts.
#
# NOTES:
#   - This file is intentionally generic: it can be used by both GUIdO and DOCNET loaders.
#   - Functions are vector-safe where needed (avoid length > 1 logical errors).
#   - Empty/irrelevant content returns NA_character_ (helps downstream translation skipping).

# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------

extract_text_from_html <- function(html_text,
                                   collapse = ";\n",
                                   col_sep  = " ",
                                   use_cache = TRUE) {
  stopifnot(is.character(html_text))
  
  # -----------------------------------------------------------------
  # Internal helpers: bullet removal + character sanitizing (vector-safe)
  # -----------------------------------------------------------------
  
  .drop_list_markers <- function(z) {
    if (!length(z)) return(z)
    na <- is.na(z); z[na] <- ""
    z <- gsub("(?m)^(\\s*)[•●◦▪·]+\\s*", "\\1", z, perl = TRUE)  # bullets
    z <- gsub("(?m)^(\\s*)[-–—]+\\s+", "\\1", z, perl = TRUE)    # dash lists
    z[na] <- NA_character_
    z
  }
  
  .sanitize_chars <- function(z) {
    if (!length(z)) return(z)
    na <- is.na(z); z[na] <- ""
    
    # Remove zero-width characters and list markers.
    z <- gsub("[\u200B\uFEFF]", "", z, perl = TRUE)
    z <- .drop_list_markers(z)
    
    # Whitelist letters/numbers/space + basic punctuation/quotes.
    allowed <- "[^\\p{L}\\p{N}\\s\\.,;:!?¿¡()\\[\\]{}'\"“”‘’/\\\\\\-–—+&%€$#@·]"
    z <- gsub(allowed, " ", z, perl = TRUE)
    
    # Compact spaces/tabs (do not collapse \n).
    z <- gsub("[ \t]+", " ", z, perl = TRUE)
    z <- trimws(z)
    
    z[na] <- NA_character_
    z
  }
  
  # -----------------------------------------------------------------
  # Normalisation helpers
  # -----------------------------------------------------------------
  
  norm_basic <- function(z) {
    z <- gsub("\u00A0", " ", z, fixed = TRUE)   # NBSP real
    z <- gsub("&nbsp;", " ", z, fixed = TRUE)  # NBSP entity
    z <- gsub("[ \t]+", " ", z, perl = TRUE)   # keep \n
    z <- trimws(z)
    .sanitize_chars(z)
  }
  
  norm_cell <- function(z) {
    z <- norm_basic(z)
    z <- gsub("\\r", "", z, fixed = TRUE)
    z <- gsub("\\n[ \t]*\\n+", "\n", z, perl = TRUE)  # remove empty lines
    trimws(z)
  }
  
  norm_text <- function(txt) {
    txt <- .sanitize_chars(txt)
    txt <- gsub("(;\\s*\\n)+", ";\n", txt, perl = TRUE)
    txt <- gsub("(^|\\n)\\s*;\\s*", "\\1", txt, perl = TRUE)
    txt <- gsub("^\\s*;\\s*", "", txt, perl = TRUE)
    trimws(txt)
  }
  
  # -----------------------------------------------------------------
  # Domain-specific label removals / note detection
  # -----------------------------------------------------------------
  
  drop_known_labels <- function(txt) {
    if (!length(txt)) return(txt)
    na <- is.na(txt); txt[na] <- ""
    txt <- gsub("(?i)\\bDESCRIPCIÓ\\s+GENERAL\\s+DE\\s+L['’]?ASSIGNATURA\\s*:?\\s*", "", txt, perl = TRUE)
    txt <- trimws(txt)
    txt[na] <- NA_character_
    txt
  }
  
  is_note_one <- function(z) {
    z <- norm_basic(z)
    grepl("^\\(\\*\\)", z, perl = TRUE) ||
      grepl("(?i)\\bla guia docent\\b", z, perl = TRUE) ||
      grepl("(?i)\\baquest document\\s+es\\s+p[uú]blic\\b", z, perl = TRUE)
  }
  
  # -----------------------------------------------------------------
  # HTML preprocessing helpers
  # -----------------------------------------------------------------
  
  strip_html_blocks <- function(x) {
    x <- gsub("(?is)<style\\b[^>]*>.*?</style>", " ", x, perl = TRUE)
    x <- gsub("(?is)<script\\b[^>]*>.*?</script>", " ", x, perl = TRUE)
    x <- gsub("(?is)<!--.*?-->", " ", x, perl = TRUE)
    x
  }
  
  # Flexible code pattern: 1–3 letters + 1–3 digits (A56, CT1, ...)
  code_pat <- "^[A-Za-z]{1,3}[0-9]{1,3}$"
  
  # Detect table header rows by their <strong> tokens (Tema/Subtema, Tipus A/B/C + Codi + Competències/Resultats)
  is_header_by_strong <- function(strongs) {
    if (!length(strongs)) return(FALSE)
    s <- tolower(norm_basic(strongs))
    
    has_tema <- any(s == "tema")
    has_sub  <- any(s == "subtema")
    if (has_tema && has_sub) return(TRUE)
    
    tipus_abc <- any(grepl("^tipus\\s*[abc]$", s, perl = TRUE))
    has_codi  <- any(s == "codi")
    has_comp  <- any(grepl("^compet", s, perl = TRUE))
    has_res   <- any(grepl("^resultat", s, perl = TRUE))
    
    (tipus_abc && has_codi && (has_comp || has_res))
  }
  
  # Clean bibliography items (drop empty, notes, fix ", ,", remove trailing commas)
  clean_biblio_item <- function(x) {
    x <- norm_basic(x)
    if (!nzchar(x) || is_note_one(x)) return("")
    x <- gsub("\\s*,\\s*,\\s*", ", ", x, perl = TRUE)
    x <- gsub("\\s*,\\s*$", "", x, perl = TRUE)
    x <- drop_known_labels(x)
    trimws(x)
  }
  
  # -----------------------------------------------------------------
  # Parser for a single HTML fragment
  # -----------------------------------------------------------------
  
  parse_one <- function(x) {
    if (is.na(x) || !nzchar(x)) return(NA_character_)
    
    # Remove non-content blocks and decode escaped HTML if needed.
    x <- strip_html_blocks(x)
    x <- gsub("&lt;",  "<", x, fixed = TRUE)
    x <- gsub("&gt;",  ">", x, fixed = TRUE)
    x <- gsub("&amp;", "&", x, fixed = TRUE)
    
    # Convert line breaks before parsing.
    x2 <- gsub("<br\\s*/?>", "\n", x, perl = TRUE)
    
    # If no table rows, fall back to tag stripping.
    if (!grepl("<\\s*tr\\b", x2, ignore.case = TRUE, perl = TRUE)) {
      txt <- gsub("<[^>]+>", " ", x2)
      txt <- drop_known_labels(txt)
      txt <- norm_text(norm_basic(txt))
      if (!nzchar(txt) || is_note_one(txt)) return(NA_character_)
      return(txt)
    }
    
    # Wrap in a container so read_html always has a root node.
    doc <- xml2::read_html(paste0("<div>", x2, "</div>"))
    trs <- xml2::xml_find_all(doc, ".//tr")
    
    if (!length(trs)) {
      txt <- norm_text(norm_basic(xml2::xml_text(doc)))
      txt <- drop_known_labels(txt)
      if (!nzchar(txt) || is_note_one(txt)) return(NA_character_)
      return(txt)
    }
    
    rows <- character(0)
    
    for (tr in trs) {
      # Skip header rows detected by <strong>.
      strongs <- xml2::xml_text(xml2::xml_find_all(tr, ".//strong"))
      if (is_header_by_strong(strongs)) next
      
      # Extract cells.
      cells <- xml2::xml_find_all(tr, ".//th|.//td")
      if (!length(cells)) next
      
      parts <- xml2::xml_text(cells)
      parts <- vapply(parts, norm_cell, character(1))
      parts <- parts[nzchar(parts)]
      if (!length(parts)) next
      
      # Drop single-cell rows that only contain bibliography labels.
      if (length(parts) == 1) {
        p1 <- tolower(norm_basic(parts[1]))
        if (p1 %in% c("bàsica", "complementària")) next
        if (is_note_one(parts[1])) next
      }
      
      # Bibliography: first cell is a label, second cell is the list.
      # We drop the label and keep items only.
      if (length(parts) >= 2) {
        p1 <- tolower(norm_basic(parts[1]))
        if (p1 %in% c("bàsica", "complementària")) {
          items <- unlist(strsplit(parts[2], "\\n+", perl = TRUE))
          items <- vapply(items, clean_biblio_item, character(1))
          items <- items[nzchar(items)]
          if (!length(items)) next
          rows <- c(rows, paste(items, collapse = "; "))
          next
        }
      }
      
      # Drop maps/notes and known labels.
      row_flat <- drop_known_labels(norm_basic(paste(parts, collapse = " ")))
      if (!nzchar(row_flat) || is_note_one(row_flat)) next
      
      # If the row includes a code token, keep "CODE + last descriptive cell".
      code_idx <- which(grepl(code_pat, parts, perl = TRUE))
      if (length(code_idx)) {
        code <- parts[code_idx[1]]
        descr_candidates <- parts[setdiff(seq_along(parts), code_idx[1])]
        descr <- if (length(descr_candidates)) tail(descr_candidates, 1) else ""
        row <- drop_known_labels(trimws(paste(code, descr)))
        row <- norm_basic(row)
        if (nzchar(row) && !is_note_one(row)) rows <- c(rows, row)
        next
      }
      
      # Default: concatenate cells with a plain separator.
      row <- drop_known_labels(norm_basic(paste(parts, collapse = col_sep)))
      if (nzchar(row) && !is_note_one(row)) rows <- c(rows, row)
    }
    
    rows <- rows[nzchar(rows)]
    if (!length(rows)) return(NA_character_)
    
    out <- norm_text(paste(rows, collapse = collapse))
    out <- drop_known_labels(out)
    if (!nzchar(out) || is_note_one(out)) NA_character_ else out
  }
  
  # -----------------------------------------------------------------
  # Vectorised execution with optional caching (fast on repeated HTML)
  # -----------------------------------------------------------------
  
  if (use_cache) {
    uniq <- unique(html_text)
    idx  <- match(html_text, uniq)
    uniq_out <- vapply(uniq, parse_one, character(1))
    return(uniq_out[idx])
  }
  
  vapply(html_text, parse_one, character(1))
}


# -------------------------------------------------------------------
# Public API: extract <a> text lists (e.g., GUIdO coordinators/professors)
# -------------------------------------------------------------------

extract_anchor_text_list_from_html <- function(html_text,
                                               sep_out = ";\n",
                                               use_cache = TRUE) {
  stopifnot(is.character(html_text))
  
  .norm_anchor <- function(z) {
    z <- gsub("\u00A0", " ", z, fixed = TRUE)
    z <- gsub("&nbsp;", " ", z, fixed = TRUE)
    z <- gsub("[ \t]+", " ", z, perl = TRUE)
    z <- trimws(z)
    z
  }
  
  .parse_one <- function(x) {
    if (length(x) != 1L || is.na(x) || !nzchar(trimws(x))) return(NA_character_)
    
    # De-escape basics (best-effort)
    x <- gsub("&lt;",  "<", x, fixed = TRUE)
    x <- gsub("&gt;",  ">", x, fixed = TRUE)
    x <- gsub("&amp;", "&", x, fixed = TRUE)
    
    doc <- xml2::read_html(paste0("<div>", x, "</div>"))
    nodes <- xml2::xml_find_all(doc, ".//a")
    
    if (!length(nodes)) return(NA_character_)
    
    txt <- xml2::xml_text(nodes)
    txt <- vapply(txt, .norm_anchor, character(1))
    txt <- txt[nzchar(txt)]
    
    if (!length(txt)) return(NA_character_)
    paste(txt, collapse = sep_out)
  }
  
  if (isTRUE(use_cache)) {
    uniq <- unique(html_text)
    idx  <- match(html_text, uniq)
    uniq_out <- vapply(uniq, .parse_one, character(1))
    return(uniq_out[idx])
  }
  
  vapply(html_text, .parse_one, character(1))
}

# Extract visible block items (li/p) from HTML and collapse as a list.
extract_block_items_from_html <- function(html_text,
                                          xpath = ".//li|.//p",
                                          collapse = ";\n",
                                          use_cache = TRUE) {
  stopifnot(is.character(html_text))
  
  parse_one <- function(x) {
    if (is.na(x) || !nzchar(x)) return(NA_character_)
    
    # Decode some common entities
    x <- gsub("&lt;",  "<", x, fixed = TRUE)
    x <- gsub("&gt;",  ">", x, fixed = TRUE)
    x <- gsub("&amp;", "&", x, fixed = TRUE)
    x <- gsub("&nbsp;", " ", x, fixed = TRUE)
    
    doc <- xml2::read_html(paste0("<div>", x, "</div>"))
    
    # Drop hidden nodes (common in GUIdO: span {display:none})
    hidden <- xml2::xml_find_all(doc, ".//*[contains(translate(@style,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'display:none')]")
    if (length(hidden)) xml2::xml_remove(hidden)
    
    nodes <- xml2::xml_find_all(doc, xpath)
    if (!length(nodes)) return(NA_character_)
    
    parts <- xml2::xml_text(nodes)
    parts <- trimws(gsub("\\s+", " ", parts, perl = TRUE))
    parts <- parts[nzchar(parts)]
    
    if (!length(parts)) return(NA_character_)
    
    out <- paste(parts, collapse = collapse)
    out <- trimws(out)
    if (!nzchar(out)) NA_character_ else out
  }
  
  if (use_cache) {
    uniq <- unique(html_text)
    idx  <- match(html_text, uniq)
    uniq_out <- vapply(uniq, parse_one, character(1))
    return(uniq_out[idx])
  }
  
  vapply(html_text, parse_one, character(1))
}

# Extract bibliography "titles" preferentially from <strong> (GUIdO), else fallback to li text.
extract_reference_titles_from_html <- function(html_text,
                                               collapse = ";\n",
                                               use_cache = TRUE) {
  stopifnot(is.character(html_text))
  
  parse_one <- function(x) {
    if (is.na(x) || !nzchar(x)) return(NA_character_)
    
    x <- gsub("&lt;",  "<", x, fixed = TRUE)
    x <- gsub("&gt;",  ">", x, fixed = TRUE)
    x <- gsub("&amp;", "&", x, fixed = TRUE)
    x <- gsub("&nbsp;", " ", x, fixed = TRUE)
    
    doc <- xml2::read_html(paste0("<div>", x, "</div>"))
    
    hidden <- xml2::xml_find_all(doc, ".//*[contains(translate(@style,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'display:none')]")
    if (length(hidden)) xml2::xml_remove(hidden)
    
    strongs <- xml2::xml_text(xml2::xml_find_all(doc, ".//li//strong"))
    strongs <- trimws(gsub("\\s+", " ", strongs, perl = TRUE))
    strongs <- strongs[nzchar(strongs)]
    
    if (length(strongs)) {
      return(paste(strongs, collapse = collapse))
    }
    
    # fallback
    lis <- xml2::xml_text(xml2::xml_find_all(doc, ".//li"))
    lis <- trimws(gsub("\\s+", " ", lis, perl = TRUE))
    lis <- lis[nzchar(lis)]
    if (!length(lis)) NA_character_ else paste(lis, collapse = collapse)
  }
  
  if (use_cache) {
    uniq <- unique(html_text)
    idx  <- match(html_text, uniq)
    uniq_out <- vapply(uniq, parse_one, character(1))
    return(uniq_out[idx])
  }
  
  vapply(html_text, parse_one, character(1))
}
