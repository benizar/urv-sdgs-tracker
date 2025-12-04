# File: src/common/04_translation_tolerant.R
# Purpose:
#   Generic "tolerant" translation helpers.
#   This module detects suspicious outputs, splits text into smaller pieces,
#   and applies a fallback strategy (full -> semicolons -> lines -> sentences).
#
# Notes:
#   - Service/API calls are NOT implemented here.
#   - The actual translation is injected via `translate_fun`, so this code is
#     reusable across translation backends and projects.

# -------------------------------------------------------------------
# Suspicious-output detection (for retries)
# -------------------------------------------------------------------
is_suspicious_translation <- function(txt) {
  if (is.null(txt) || is.na(txt) || !is.character(txt) || !nzchar(txt)) {
    return(FALSE)
  }
  
  # 1) very repetitive tokens with spaces
  tokens <- strsplit(txt, "\\s+")[[1]]
  if (length(tokens)) {
    tab <- table(tokens)
    if (length(tab) && max(tab) > 50L) {
      return(TRUE)
    }
  }
  
  # 2) very long string with almost no whitespace / repeated pattern
  txt_nospace <- gsub("\\s+", "", txt)
  n_chars     <- nchar(txt_nospace, allowNA = TRUE, keepNA = FALSE)
  
  if (!is.na(n_chars) && n_chars > 200L) {
    # repeated pattern like "mainstremainstremainstrem..."
    if (grepl("^(.{3,30})\\1{10,}$", txt_nospace, perl = TRUE)) {
      return(TRUE)
    }
    
    # fallback: extremely low whitespace ratio
    m <- gregexpr("\\s", txt)[[1]]
    n_spaces <- if (identical(m, -1L)) 0L else length(m)
    if (n_spaces == 0L || (n_spaces / nchar(txt)) < 0.005) {
      return(TRUE)
    }
  }
  
  FALSE
}

# -------------------------------------------------------------------
# Split helpers (tolerant fallback) - internal
# -------------------------------------------------------------------
.split_semicolons <- function(x) {
  if (is.null(x) || is.na(x)) return(character(0))
  parts <- unlist(strsplit(x, "\\s*;\\s*", perl = TRUE))
  parts <- trimws(parts)
  parts[nzchar(parts)]
}

.split_lines <- function(x) {
  if (is.null(x) || is.na(x)) return(character(0))
  parts <- unlist(strsplit(x, "\\r?\\n+", perl = TRUE))
  parts <- trimws(parts)
  parts[nzchar(parts)]
}

.split_sentences <- function(x) {
  if (is.null(x) || is.na(x)) return(character(0))
  s <- unlist(strsplit(x, "(?<=[.!?])\\s+", perl = TRUE))
  s <- trimws(s)
  s[nzchar(s)]
}

.choose_semicolon_joiner <- function(original_text) {
  if (is.null(original_text) || is.na(original_text)) return("; ")
  if (grepl(";\\s*\\n", original_text, perl = TRUE)) ";\n" else "; "
}

# -------------------------------------------------------------------
# Tolerant fallback: partial translation + counters/ratios
# Order:
#   full text -> semicolons -> lines -> sentences
# Failed pieces are kept in ORIGINAL form (not "Translation Error").
# -------------------------------------------------------------------
translate_tolerant <- function(
    text,
    source_lang,
    target_lang,
    service,
    translate_fun,
    first_attempt = NULL
) {
  if (!is.function(translate_fun)) {
    stop("translate_tolerant(): `translate_fun` must be a function.", call. = FALSE)
  }
  
  out <- list(
    translated_text        = NA_character_,
    difficulty             = "none",
    split_strategy         = "full",
    parts_total            = 1L,
    failed_parts           = 0L,
    failed_parts_ratio     = 0,
    sentences_total        = 0L,
    failed_sentences       = 0L,
    failed_sentences_ratio = 0
  )
  
  if (is.null(text) || is.na(text) || !nzchar(trimws(text))) {
    out$translated_text <- NA_character_
    return(out)
  }
  
  # 1) Full attempt (re-use if given)
  tr_full <- if (!is.null(first_attempt)) first_attempt else translate_fun(
    text,
    source_lang,
    target_lang,
    service
  )
  
  full_ok <- !identical(tr_full, "Translation Error") && !is_suspicious_translation(tr_full)
  
  if (full_ok) {
    out$translated_text <- tr_full
    out$difficulty <- "none"
    out$split_strategy <- "full"
    out$parts_total <- 1L
    return(out)
  }
  
  # Semicolon level
  parts <- .split_semicolons(text)
  if (!length(parts)) parts <- trimws(text)
  
  parts <- parts[nzchar(parts)]
  if (!length(parts)) {
    out$translated_text <- "Translation Error"
    out$difficulty <- "hard"
    out$split_strategy <- "full"
    return(out)
  }
  
  out$parts_total <- as.integer(length(parts))
  sem_joiner <- .choose_semicolon_joiner(text)
  
  part_out <- character(length(parts))
  part_failed <- logical(length(parts))
  any_translation_success <- FALSE
  
  used_semicolon <- length(parts) > 1L
  used_lines <- FALSE
  used_sentences <- FALSE
  
  for (i in seq_along(parts)) {
    p <- parts[[i]]
    
    tr_p <- translate_fun(p, source_lang, target_lang, service)
    ok_p <- !identical(tr_p, "Translation Error") && !is_suspicious_translation(tr_p)
    
    if (ok_p) {
      part_out[[i]] <- tr_p
      any_translation_success <- TRUE
      next
    }
    
    # Lines fallback (only for this part)
    used_lines <- TRUE
    lines <- .split_lines(p)
    if (!length(lines)) lines <- p
    
    line_out <- character(length(lines))
    line_had_fail <- logical(length(lines))
    
    for (j in seq_along(lines)) {
      ln <- lines[[j]]
      
      tr_ln <- translate_fun(ln, source_lang, target_lang, service)
      ok_ln <- !identical(tr_ln, "Translation Error") && !is_suspicious_translation(tr_ln)
      
      if (ok_ln) {
        line_out[[j]] <- tr_ln
        any_translation_success <- TRUE
        next
      }
      
      # Sentence fallback (only for this line)
      used_sentences <- TRUE
      sents <- .split_sentences(ln)
      if (!length(sents)) sents <- ln
      
      out$sentences_total <- out$sentences_total + as.integer(length(sents))
      
      sent_out <- character(length(sents))
      any_fail_here <- FALSE
      
      for (k in seq_along(sents)) {
        s <- sents[[k]]
        tr_s <- translate_fun(s, source_lang, target_lang, service)
        ok_s <- !identical(tr_s, "Translation Error") && !is_suspicious_translation(tr_s)
        
        if (ok_s) {
          sent_out[[k]] <- tr_s
          any_translation_success <- TRUE
        } else {
          sent_out[[k]] <- s # keep original
          out$failed_sentences <- out$failed_sentences + 1L
          any_fail_here <- TRUE
        }
      }
      
      line_out[[j]] <- paste(sent_out, collapse = " ")
      line_had_fail[[j]] <- any_fail_here
    }
    
    # Rebuild part with line breaks (preserve list-like structure)
    part_out[[i]] <- paste(line_out, collapse = "\n")
    part_failed[[i]] <- any(line_had_fail)
  }
  
  out$translated_text <- paste(part_out, collapse = if (used_semicolon) sem_joiner else "\n")
  
  out$failed_parts <- as.integer(sum(part_failed))
  out$failed_parts_ratio <- if (out$parts_total > 0L) out$failed_parts / out$parts_total else 0
  out$failed_sentences_ratio <- if (out$sentences_total > 0L) out$failed_sentences / out$sentences_total else 0
  
  out$split_strategy <- paste(
    c(
      if (used_semicolon) "semicolon" else NULL,
      if (used_lines)     "line"      else NULL,
      if (used_sentences) "sentence"  else NULL
    ),
    collapse = ">"
  )
  if (!nzchar(out$split_strategy)) out$split_strategy <- "full"
  
  if (!isTRUE(any_translation_success)) {
    out$difficulty <- "hard"
  } else if (out$failed_parts > 0L || out$failed_sentences > 0L) {
    out$difficulty <- "partial"
  } else {
    out$difficulty <- "none"
  }
  
  out
}
