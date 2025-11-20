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

# Return the base URL for the selected translation service.
#
# The default values assume a Docker Compose setup where services are
# named "libretranslate" and "apertium" on the default project network.
#
# Users can override these defaults via environment variables:
#   - LIBRETRANSLATE_URL
#   - APERTIUM_URL
#
# Examples:
#   get_translation_base_url("libretranslate")
#   get_translation_base_url("apertium")
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

#' Translate a specific column in a data frame using LibreTranslate
translate_column <- function(
    df,
    column,
    source_lang = "auto",
    target_lang = "en",
    file_path = "output.csv",
    batch_size = 100,
    max_cores = 4,   # kept for API compatibility, but not used internally
    context = NULL,
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
        id               = character(),
        original         = character(),
        detected_language = character(),
        confidence       = numeric(),
        translated_text  = character(),
        stringsAsFactors = FALSE
      ),
      file   = file_path,
      append = FALSE
    )
  }
  
  # Normalise text (optional heuristic).
  df[[column]] <- tolower(df[[column]])
  
  # Optionally prepend context for short texts.
  if (!is.null(context)) {
    df[[column]] <- paste0(context, df[[column]])
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
      return(list(detectedLanguage = NA_character_, confidence = NA_real_))
    })
    
    if (!is.null(response) && httr::status_code(response) == 200L) {
      json <- jsonlite::fromJSON(
        httr::content(response, "text", encoding = "UTF-8")
      )
      return(list(
        detectedLanguage = json$language[1],
        confidence       = json$confidence[1]
      ))
    }
    
    list(detectedLanguage = NA_character_, confidence = NA_real_)
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
      return(list(detectedLanguage = NA_character_, confidence = NA_real_))
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
      return(list(detectedLanguage = NA_character_, confidence = NA_real_))
    })
    
    if (length(content) == 0L || !is.list(content)) {
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
      return("Error in translation request")
    })
    
    if (!is.null(response) && httr::status_code(response) == 200L) {
      json <- jsonlite::fromJSON(
        httr::content(response, "text", encoding = "UTF-8")
      )
      return(json$translatedText)
    }
    
    "Error in translation request"
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
      return(NULL)
    })
    
    if (!is.null(response) && httr::status_code(response) == 200L) {
      json <- jsonlite::fromJSON(
        httr::content(response, "text", encoding = "UTF-8")
      )
      return(json$responseData$translatedText)
    }
    
    "Translation Error"
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
  # Batch processing (sequential)
  # ------------------------------------------------------------------
  n <- nrow(df)
  if (n == 0L) {
    message("translate_column(): input data frame has 0 rows, nothing to do.")
    return(invisible(NULL))
  }
  
  batch_ids <- split(seq_len(n), ceiling(seq_len(n) / batch_size))
  n_batches <- length(batch_ids)
  
  message(
    "translate_column(): processing ", n,
    " rows in ", n_batches,
    " batches using service = ", service, "."
  )
  
  batch_counter <- 0L
  
  for (idx in batch_ids) {
    batch_counter <- batch_counter + 1L
    message("  - batch ", batch_counter, " / ", n_batches)
    
    batch <- df[idx, c(id_column, column), drop = FALSE]
    
    results <- lapply(seq_len(nrow(batch)), function(i) {
      text   <- batch[[column]][i]
      row_id <- batch[[id_column]][i]
      
      if (is.null(text) || is.na(text) || text == "" || trimws(text) == "") {
        return(data.frame(
          id               = row_id,
          original         = NA_character_,
          detected_language = NA_character_,
          confidence       = NA_real_,
          translated_text  = NA_character_,
          stringsAsFactors = FALSE
        ))
      }
      
      detected <- detect_language(text, service)
      final_source_lang <- if (source_lang == "auto") {
        detected$detectedLanguage
      } else {
        source_lang
      }
      
      if (service == "apertium") {
        final_source_lang <- convert_lang_code(final_source_lang, service)
      }
      
      translated_text <- translate_text(text, final_source_lang, target_lang, service)
      
      # If context was used, strip it after translation.
      if (!is.null(context)) {
        pattern        <- "^\\s*[^:]+:\\s*"
        text           <- sub(pattern, "", text)
        translated_text <- sub(pattern, "", translated_text)
      }
      
      data.frame(
        id               = row_id,
        original         = text,
        detected_language = detected$detectedLanguage,
        confidence       = detected$confidence,
        translated_text  = translated_text,
        stringsAsFactors = FALSE
      )
    })
    
    results_df <- do.call(rbind, results)
    
    data.table::fwrite(
      results_df,
      file     = file_path,
      append   = TRUE,
      col.names = !file.exists(file_path) || (file.info(file_path)$size == 0L)
    )
  }
  
  message("translate_column(): translation process completed.")
  invisible(NULL)
}



