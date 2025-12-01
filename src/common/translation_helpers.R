# File: src/common/translation_helpers.R
# Common helpers for translation services (LibreTranslate / Apertium).
# Shared by pipeline targets and standalone scripts.

# -------------------------------------------------------------------
# Utility: right-trim slashes from URLs
# -------------------------------------------------------------------
rtrim_slash <- function(x) {
  sub("/+$", "", x)
}

# -------------------------------------------------------------------
# Service healthcheck (no Docker start here)
# -------------------------------------------------------------------
# Check that the configured translation service is reachable.
# - translate_cfg$service: "libretranslate" or "apertium" (default: "libretranslate")
# - translate_cfg$check_service: if FALSE, skip the healthcheck
# - translate_cfg$libretranslate_base_url: override base URL
# - translate_cfg$apertium_base_url: override base URL
# - translate_cfg$healthcheck_path: path to ping (default: "/")
# - translate_cfg$healthcheck_timeout_sec: total timeout in seconds (default: 60)
# - translate_cfg$healthcheck_poll_interval_sec: pause between attempts (default: 2)
check_translation_service <- function(translate_cfg) {
  if (!isTRUE(translate_cfg$check_service %||% TRUE)) {
    message("Translation service healthcheck disabled (check_service = FALSE).")
    return(invisible(NULL))
  }
  
  service <- tolower(translate_cfg$service %||% "libretranslate")
  
  # 1) choose base URL from config overrides if present
  # 2) otherwise, fall back to get_translation_base_url()
  base_url <- switch(
    service,
    "libretranslate" = {
      translate_cfg$libretranslate_base_url %||%
        get_translation_base_url("libretranslate")
    },
    "apertium" = {
      translate_cfg$apertium_base_url %||%
        get_translation_base_url("apertium")
    },
    {
      stop("Unknown translation service in config: ", service)
    }
  )
  
  health_path <- translate_cfg$healthcheck_path %||% "/"
  timeout     <- translate_cfg$healthcheck_timeout_sec %||% 60
  interval    <- translate_cfg$healthcheck_poll_interval_sec %||% 2
  
  url <- paste0(rtrim_slash(base_url), health_path)
  
  message("Checking translation service at: ", url)
  
  deadline <- Sys.time() + timeout
  
  repeat {
    ok <- FALSE
    
    try({
      resp <- httr::GET(url)
      if (httr::status_code(resp) %in% c(200L, 204L)) {
        ok <- TRUE
      }
    }, silent = TRUE)
    
    if (ok) {
      message("Translation service is up.")
      return(invisible(NULL))
    }
    
    if (Sys.time() >= deadline) {
      stop(
        "Translation service at ", url,
        " did not respond with 200/204 within ",
        timeout, " seconds.\n",
        "Make sure the Docker services are running, for example:\n",
        "  docker compose up libretranslate or docker compose up apertium\n",
        "and then re-run the pipeline."
      )
    }
    
    Sys.sleep(interval)
  }
}

# -------------------------------------------------------------------
# Base URL helper
# -------------------------------------------------------------------
# The default values assume a Docker Compose setup where services are
# named "libretranslate" and "apertium" on the default project network.
#
# Users can override these defaults via environment variables:
#   - LIBRETRANSLATE_URL
#   - APERTIUM_URL
get_translation_base_url <- function(service = c("libretranslate", "apertium")) {
  service <- match.arg(service)
  
  base <- switch(
    service,
    "libretranslate" = Sys.getenv(
      "LIBRETRANSLATE_URL",
      unset = "http://libretranslate:5000"
    ),
    "apertium" = Sys.getenv(
      "APERTIUM_URL",
      unset = "http://apertium:2737"
    )
  )
  
  # Normalise: remove trailing slashes just in case.
  sub("/+$", "", base)
}

# -------------------------------------------------------------------
# Translate a specific column in a data frame
# -------------------------------------------------------------------
translate_column <- function(
    df,
    column,
    source_lang = "auto",
    target_lang = "en",
    file_path = "output.csv",
    batch_size = 100,
    max_cores = 4,      # kept for API compatibility, but not used internally
    context = NULL,     # legacy: treated as a prefix
    context_prefix = NULL,
    context_suffix = NULL,
    id_column = NULL,
    service = "libretranslate"
) {
  # We do not create clusters and we do not close global connections here.
  # Parallelism should be handled by the caller (e.g. {targets}).
  
  if (!column %in% colnames(df)) {
    stop("translate_column(): column '", column,
         "' does not exist in the provided data frame.")
  }
  
  # Initialise output file if it does not exist.
  if (!file.exists(file_path)) {
    data.table::fwrite(
      data.frame(
        id                    = character(),
        original              = character(),
        detected_language     = character(),
        confidence            = numeric(),
        translated_text       = character(),
        difficulty            = character(),
        split_strategy        = character(),
        parts_total           = integer(),
        failed_parts          = integer(),
        failed_parts_ratio    = numeric(),
        sentences_total       = integer(),
        failed_sentences      = integer(),
        failed_sentences_ratio= numeric(),
        stringsAsFactors      = FALSE
      ),
      file   = file_path,
      append = FALSE
    )
  }
  
  # Stable ID column.
  if (is.null(id_column)) {
    df$id_temp <- seq_len(nrow(df))
    id_column  <- "id_temp"
  } else if (!id_column %in% colnames(df)) {
    stop("translate_column(): specified id_column '", id_column,
         "' does not exist in the data frame.")
  }
  
  # ------------------------------------------------------------------
  # Helper: convert language codes to Apertium format
  # ------------------------------------------------------------------
  convert_lang_code <- function(lang, service) {
    if (service == "apertium") {
      lang_map <- list(
        "ca" = "cat",
        "es" = "spa",
        "en" = "eng",
        "fr" = "fra",
        "de" = "deu"
      )
      if (lang %in% names(lang_map)) {
        return(lang_map[[lang]])
      }
    }
    lang
  }
  
  # ------------------------------------------------------------------
  # Language detection for LibreTranslate
  # ------------------------------------------------------------------
  detect_language_libretranslate <- function(text) {
    base_url <- get_translation_base_url("libretranslate")
    url      <- paste0(base_url, "/detect")
    
    response <- tryCatch({
      httr::POST(
        url,
        body   = list(q = text),
        encode = "json",
        httr::accept_json()
      )
    }, error = function(e) {
      message("LibreTranslate detect error: ", e$message)
      NULL
    })
    
    if (is.null(response) ||
        !inherits(response, "response") ||
        httr::status_code(response) != 200L) {
      message("LibreTranslate detect: invalid HTTP response")
      return(list(detectedLanguage = NA_character_, confidence = NA_real_))
    }
    
    json <- tryCatch({
      jsonlite::fromJSON(
        httr::content(response, "text", encoding = "UTF-8")
      )
    }, error = function(e) {
      message("LibreTranslate detect: JSON parse error: ", e$message)
      NULL
    })
    
    if (is.null(json) || is.null(json$language) || !length(json$language)) {
      return(list(detectedLanguage = NA_character_, confidence = NA_real_))
    }
    
    list(
      detectedLanguage = json$language[1],
      confidence       = json$confidence[1]
    )
  }
  
  # ------------------------------------------------------------------
  # Language detection for Apertium
  # ------------------------------------------------------------------
  detect_language_apertium <- function(text) {
    base_url <- get_translation_base_url("apertium")
    url      <- paste0(base_url, "/identifyLang")
    
    response <- tryCatch({
      httr::GET(url, query = list(q = text))
    }, error = function(e) {
      message("Apertium detect error: ", e$message)
      NULL
    })
    
    if (is.null(response) ||
        !inherits(response, "response") ||
        httr::status_code(response) != 200L) {
      message("Apertium detect: invalid HTTP response")
      return(list(detectedLanguage = NA_character_, confidence = NA_real_))
    }
    
    content <- tryCatch({
      jsonlite::fromJSON(
        httr::content(response, "text", encoding = "UTF-8")
      )
    }, error = function(e) {
      message("Apertium detect: JSON parse error: ", e$message)
      NULL
    })
    
    if (is.null(content) || length(content) == 0L || !is.list(content)) {
      message("Apertium detect: empty or malformed response")
      return(list(detectedLanguage = NA_character_, confidence = NA_real_))
    }
    
    lang_conf <- unlist(content)
    
    if (length(lang_conf) > 0L) {
      best_lang       <- names(lang_conf)[which.max(lang_conf)]
      best_confidence <- max(lang_conf)
    } else {
      best_lang       <- NA_character_
      best_confidence <- NA_real_
    }
    
    list(detectedLanguage = best_lang, confidence = best_confidence)
  }
  
  # ------------------------------------------------------------------
  # Generic detection dispatcher
  # ------------------------------------------------------------------
  detect_language <- function(text, service) {
    if (service == "libretranslate") {
      detect_language_libretranslate(text)
    } else if (service == "apertium") {
      detect_language_apertium(text)
    } else {
      stop("translate_column(): invalid service. Use 'libretranslate' or 'apertium'.")
    }
  }
  
  # ------------------------------------------------------------------
  # Translation with LibreTranslate
  # ------------------------------------------------------------------
  translate_text_libretranslate <- function(text, source_lang, target_lang) {
    base_url <- get_translation_base_url("libretranslate")
    url      <- paste0(base_url, "/translate")
    
    response <- tryCatch({
      httr::POST(
        url,
        body   = list(
          q      = text,
          source = source_lang,
          target = target_lang,
          format = "text"
        ),
        encode = "json",
        httr::accept_json()
      )
    }, error = function(e) {
      message("LibreTranslate translation error: ", e$message)
      NULL
    })
    
    if (is.null(response) ||
        !inherits(response, "response") ||
        httr::status_code(response) != 200L) {
      message("LibreTranslate translation: invalid HTTP response")
      return("Translation Error")
    }
    
    json <- tryCatch({
      jsonlite::fromJSON(
        httr::content(response, "text", encoding = "UTF-8")
      )
    }, error = function(e) {
      message("LibreTranslate translation: JSON parse error: ", e$message)
      NULL
    })
    
    if (is.null(json) || is.null(json$translatedText)) {
      return("Translation Error")
    }
    
    json$translatedText
  }
  
  # ------------------------------------------------------------------
  # Translation with Apertium
  # ------------------------------------------------------------------
  translate_text_apertium <- function(text, source_lang, target_lang) {
    base_url <- get_translation_base_url("apertium")
    url      <- paste0(base_url, "/translate")
    
    source_lang <- convert_lang_code(source_lang, "apertium")
    target_lang <- convert_lang_code(target_lang, "apertium")
    
    response <- tryCatch({
      httr::GET(
        url,
        query = list(
          q        = text,
          langpair = paste(source_lang, target_lang, sep = "|")
        )
      )
    }, error = function(e) {
      message("Apertium translation error: ", e$message)
      NULL
    })
    
    if (is.null(response) ||
        !inherits(response, "response") ||
        httr::status_code(response) != 200L) {
      message("Apertium translation: invalid HTTP response")
      return("Translation Error")
    }
    
    json <- tryCatch({
      jsonlite::fromJSON(
        httr::content(response, "text", encoding = "UTF-8")
      )
    }, error = function(e) {
      message("Apertium translation: JSON parse error: ", e$message)
      NULL
    })
    
    if (is.null(json) || is.null(json$responseData$translatedText)) {
      return("Translation Error")
    }
    
    json$responseData$translatedText
  }
  
  # ------------------------------------------------------------------
  # Generic translation dispatcher
  # ------------------------------------------------------------------
  translate_text <- function(text, source_lang, target_lang, service) {
    if (service == "libretranslate") {
      translate_text_libretranslate(text, source_lang, target_lang)
    } else if (service == "apertium") {
      translate_text_apertium(text, source_lang, target_lang)
    } else {
      stop("translate_column(): invalid translation service.")
    }
  }
  
  # ------------------------------------------------------------------
  # Suspicious-output detection (for retries)
  # ------------------------------------------------------------------
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
  
  # ------------------------------------------------------------------
  # Split helpers (tolerant fallback)
  # ------------------------------------------------------------------
  split_semicolons <- function(x) {
    if (is.null(x) || is.na(x)) return(character(0))
    parts <- unlist(strsplit(x, "\\s*;\\s*", perl = TRUE))
    parts <- trimws(parts)
    parts[nzchar(parts)]
  }
  
  split_lines <- function(x) {
    if (is.null(x) || is.na(x)) return(character(0))
    parts <- unlist(strsplit(x, "\\r?\\n+", perl = TRUE))
    parts <- trimws(parts)
    parts[nzchar(parts)]
  }
  
  split_sentences <- function(x) {
    if (is.null(x) || is.na(x)) return(character(0))
    s <- unlist(strsplit(x, "(?<=[.!?])\\s+", perl = TRUE))
    s <- trimws(s)
    s[nzchar(s)]
  }
  
  choose_semicolon_joiner <- function(original_text) {
    if (is.null(original_text) || is.na(original_text)) return("; ")
    if (grepl(";\\s*\\n", original_text, perl = TRUE)) ";\n" else "; "
  }
  
  # ------------------------------------------------------------------
  # Tolerant fallback: partial translation + counters/ratios
  # Order:
  #   full text -> semicolons -> lines -> sentences
  # Failed pieces are kept in ORIGINAL form (not "Translation Error").
  # ------------------------------------------------------------------
  translate_tolerant <- function(text, source_lang, target_lang, service, first_attempt = NULL) {
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
    tr_full <- if (!is.null(first_attempt)) first_attempt else translate_text(text, source_lang, target_lang, service)
    full_ok <- !identical(tr_full, "Translation Error") && !is_suspicious_translation(tr_full)
    
    if (full_ok) {
      out$translated_text <- tr_full
      out$difficulty <- "none"
      out$split_strategy <- "full"
      out$parts_total <- 1L
      return(out)
    }
    
    # Semicolon level
    parts <- split_semicolons(text)
    if (!length(parts)) parts <- trimws(text)
    
    parts <- parts[nzchar(parts)]
    if (!length(parts)) {
      out$translated_text <- "Translation Error"
      out$difficulty <- "hard"
      out$split_strategy <- "full"
      return(out)
    }
    
    out$parts_total <- as.integer(length(parts))
    sem_joiner <- choose_semicolon_joiner(text)
    
    part_out <- character(length(parts))
    part_failed <- logical(length(parts))
    any_translation_success <- FALSE
    
    used_semicolon <- length(parts) > 1L
    used_lines <- FALSE
    used_sentences <- FALSE
    
    for (i in seq_along(parts)) {
      p <- parts[[i]]
      
      tr_p <- translate_text(p, source_lang, target_lang, service)
      ok_p <- !identical(tr_p, "Translation Error") && !is_suspicious_translation(tr_p)
      
      if (ok_p) {
        part_out[[i]] <- tr_p
        any_translation_success <- TRUE
        next
      }
      
      # Lines fallback (only for this part)
      used_lines <- TRUE
      lines <- split_lines(p)
      if (!length(lines)) lines <- p
      
      line_out <- character(length(lines))
      line_had_fail <- logical(length(lines))
      
      for (j in seq_along(lines)) {
        ln <- lines[[j]]
        
        tr_ln <- translate_text(ln, source_lang, target_lang, service)
        ok_ln <- !identical(tr_ln, "Translation Error") && !is_suspicious_translation(tr_ln)
        
        if (ok_ln) {
          line_out[[j]] <- tr_ln
          any_translation_success <- TRUE
          next
        }
        
        # Sentence fallback (only for this line)
        used_sentences <- TRUE
        sents <- split_sentences(ln)
        if (!length(sents)) sents <- ln
        
        out$sentences_total <- out$sentences_total + as.integer(length(sents))
        
        sent_out <- character(length(sents))
        any_fail_here <- FALSE
        
        for (k in seq_along(sents)) {
          s <- sents[[k]]
          tr_s <- translate_text(s, source_lang, target_lang, service)
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
    
    # Difficulty:
    # - "hard": no successful translation at all or lots of failures
    # - "partial": some translation but some failures
    # - "none": everything translated via splitting (no failures left)
    if (!isTRUE(any_translation_success)) {
      out$difficulty <- "hard"
    } else if (out$failed_parts > 0L || out$failed_sentences > 0L) {
      out$difficulty <- "partial"
    } else {
      out$difficulty <- "none"
    }
    
    out
  }
  
  # ------------------------------------------------------------------
  # Batch processing (sequential)
  # ------------------------------------------------------------------
  n <- nrow(df)
  if (n == 0L) {
    message("translate_column(): input data frame has 0 rows, nothing to do.")
    return(invisible(NULL))
  }
  
  # context (legacy) is treated as a prefix if provided
  if (!is.null(context) && is.null(context_prefix)) {
    context_prefix <- context
  }
  
  batch_ids <- split(seq_len(n), ceiling(seq_len(n) / batch_size))
  n_batches <- length(batch_ids)
  
  message(
    "translate_column(): processing ", n,
    " rows in ", n_batches,
    " batches using service = ", service, "."
  )
  
  # retry parameters for suspicious outputs / hard errors
  retry_sleep_sec <- 5
  
  batch_counter <- 0L
  
  for (idx in batch_ids) {
    batch_counter <- batch_counter + 1L
    message("  - batch ", batch_counter, " / ", n_batches)
    
    batch <- df[idx, c(id_column, column), drop = FALSE]
    
    results <- lapply(seq_len(nrow(batch)), function(i) {
      text   <- batch[[column]][i]
      row_id <- batch[[id_column]][i]
      
      # Skip empty / missing text
      if (is.null(text) || is.na(text) || trimws(text) == "") {
        return(data.frame(
          id                    = row_id,
          original              = NA_character_,
          detected_language     = NA_character_,
          confidence            = NA_real_,
          translated_text       = NA_character_,
          difficulty            = NA_character_,
          split_strategy         = NA_character_,
          parts_total           = NA_integer_,
          failed_parts          = NA_integer_,
          failed_parts_ratio    = NA_real_,
          sentences_total       = NA_integer_,
          failed_sentences      = NA_integer_,
          failed_sentences_ratio= NA_real_,
          stringsAsFactors      = FALSE
        ))
      }
      
      # Apply optional prefix/suffix only to the text sent to the service
      text_for_translate <- text
      if (!is.null(context_prefix) || !is.null(context_suffix)) {
        text_for_translate <- paste0(
          context_prefix %||% "",
          text_for_translate,
          context_suffix %||% ""
        )
      }
      
      detected <- detect_language(text_for_translate, service)
      final_source_lang <- if (source_lang == "auto") {
        detected$detectedLanguage
      } else {
        source_lang
      }
      
      # Defaults
      translated_text <- "Translation Error"
      difficulty <- "hard"
      split_strategy <- "full"
      parts_total <- 1L
      failed_parts <- 1L
      failed_parts_ratio <- 1
      sentences_total <- 0L
      failed_sentences <- 0L
      failed_sentences_ratio <- 0
      
      if (!is.null(final_source_lang) && !is.na(final_source_lang) && nzchar(final_source_lang)) {
        # First attempt: full text
        first <- translate_text(
          text_for_translate,
          final_source_lang,
          target_lang,
          service
        )
        
        ok_first <- !identical(first, "Translation Error") && !is_suspicious_translation(first)
        
        if (ok_first) {
          translated_text <- first
          difficulty <- "none"
          split_strategy <- "full"
          parts_total <- 1L
          failed_parts <- 0L
          failed_parts_ratio <- 0
          sentences_total <- 0L
          failed_sentences <- 0L
          failed_sentences_ratio <- 0
        } else {
          # Backoff then tolerant fallback (partial translation allowed)
          Sys.sleep(retry_sleep_sec)
          tol <- translate_tolerant(
            text = text_for_translate,
            source_lang = final_source_lang,
            target_lang = target_lang,
            service = service,
            first_attempt = first
          )
          
          translated_text <- tol$translated_text
          difficulty <- tol$difficulty
          split_strategy <- tol$split_strategy
          parts_total <- as.integer(tol$parts_total)
          failed_parts <- as.integer(tol$failed_parts)
          failed_parts_ratio <- as.numeric(tol$failed_parts_ratio)
          sentences_total <- as.integer(tol$sentences_total)
          failed_sentences <- as.integer(tol$failed_sentences)
          failed_sentences_ratio <- as.numeric(tol$failed_sentences_ratio)
          
          # If *even the tolerant path* yields NA/empty, keep a hard error marker.
          if (is.null(translated_text) || is.na(translated_text) || !nzchar(trimws(translated_text))) {
            translated_text <- "Translation Error"
            difficulty <- "hard"
          }
        }
      }
      
      data.frame(
        id                    = row_id,
        original              = text,  # original text without prefix/suffix
        detected_language     = detected$detectedLanguage,
        confidence            = detected$confidence,
        translated_text       = translated_text,
        difficulty            = difficulty,
        split_strategy        = split_strategy,
        parts_total           = parts_total,
        failed_parts          = failed_parts,
        failed_parts_ratio    = failed_parts_ratio,
        sentences_total       = sentences_total,
        failed_sentences      = failed_sentences,
        failed_sentences_ratio= failed_sentences_ratio,
        stringsAsFactors      = FALSE
      )
    })
    
    results_df <- do.call(rbind, results)
    
    data.table::fwrite(
      results_df,
      file      = file_path,
      append    = TRUE,
      col.names = !file.exists(file_path) || (file.info(file_path)$size == 0L)
    )
  }
  
  message("translate_column(): translation process completed.")
  invisible(NULL)
}
