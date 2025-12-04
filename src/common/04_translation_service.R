# File: src/common/04_translation_service.R
# Purpose:
#   Translation service utilities shared across the project:
#   - service healthcheck
#   - language detection (dispatcher)
#   - translation requests (dispatcher)
#
# Public API:
#   - check_translation_service()
#   - detect_language()
#   - translate_text()
#
# Notes:
#   - This module implements the HTTP client calls for LibreTranslate and Apertium.
#   - Base URLs are provided by get_translation_base_url() and normalised via rtrim_slash().

# -------------------------------------------------------------------
# Service healthcheck (no Docker start here)
# -------------------------------------------------------------------
check_translation_service <- function(translate_cfg) {
  if (!isTRUE(translate_cfg$check_service %||% TRUE)) {
    message("Translation service healthcheck disabled (check_service = FALSE).")
    return(invisible(NULL))
  }
  
  service <- tolower(translate_cfg$service %||% "libretranslate")
  
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
      stop("Unknown translation service in config: ", service, call. = FALSE)
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
      if (httr::status_code(resp) %in% c(200L, 204L)) ok <- TRUE
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
        "and then re-run the pipeline.",
        call. = FALSE
      )
    }
    
    Sys.sleep(interval)
  }
}

# ------------------------------------------------------------------
# Internal helpers - service specifics
# ------------------------------------------------------------------
.convert_lang_code <- function(lang, service) {
  if (service == "apertium") {
    lang_map <- list(
      "ca" = "cat",
      "es" = "spa",
      "en" = "eng",
      "fr" = "fra",
      "de" = "deu"
    )
    if (lang %in% names(lang_map)) return(lang_map[[lang]])
  }
  lang
}

.detect_language_libretranslate <- function(text) {
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
    jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
  }, error = function(e) {
    message("LibreTranslate detect: JSON parse error: ", e$message)
    NULL
  })
  
  if (is.null(json) || is.null(json$language) || !length(json$language)) {
    return(list(detectedLanguage = NA_character_, confidence = NA_real_))
  }
  
  list(detectedLanguage = json$language[1], confidence = json$confidence[1])
}

.detect_language_apertium <- function(text) {
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
    jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
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

.translate_text_libretranslate <- function(text, source_lang, target_lang) {
  base_url <- get_translation_base_url("libretranslate")
  url      <- paste0(base_url, "/translate")
  
  response <- tryCatch({
    httr::POST(
      url,
      body   = list(q = text, source = source_lang, target = target_lang, format = "text"),
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
    jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
  }, error = function(e) {
    message("LibreTranslate translation: JSON parse error: ", e$message)
    NULL
  })
  
  if (is.null(json) || is.null(json$translatedText)) return("Translation Error")
  json$translatedText
}

.translate_text_apertium <- function(text, source_lang, target_lang) {
  base_url <- get_translation_base_url("apertium")
  url      <- paste0(base_url, "/translate")
  
  source_lang <- .convert_lang_code(source_lang, "apertium")
  target_lang <- .convert_lang_code(target_lang, "apertium")
  
  response <- tryCatch({
    httr::GET(url, query = list(q = text, langpair = paste(source_lang, target_lang, sep = "|")))
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
    jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
  }, error = function(e) {
    message("Apertium translation: JSON parse error: ", e$message)
    NULL
  })
  
  if (is.null(json) || is.null(json$responseData$translatedText)) return("Translation Error")
  json$responseData$translatedText
}

# ------------------------------------------------------------------
# Public API - dispatchers
# ------------------------------------------------------------------
detect_language <- function(text, service) {
  if (service == "libretranslate") {
    .detect_language_libretranslate(text)
  } else if (service == "apertium") {
    .detect_language_apertium(text)
  } else {
    stop("detect_language(): invalid service. Use 'libretranslate' or 'apertium'.",
         call. = FALSE)
  }
}

translate_text <- function(text, source_lang, target_lang, service) {
  if (service == "libretranslate") {
    .translate_text_libretranslate(text, source_lang, target_lang)
  } else if (service == "apertium") {
    .translate_text_apertium(text, source_lang, target_lang)
  } else {
    stop("translate_text(): invalid translation service.", call. = FALSE)
  }
}
