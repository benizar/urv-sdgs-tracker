#' Translate a specific column in a data frame using LibreTranslate
#'
#' This function detects the language of a specified column in a data frame (if `source_lang = "auto"`)
#' and translates its content into a target language using the LibreTranslate API. It writes the results
#' directly to a CSV file in batches to optimize memory usage and provides a progress bar.
#'
#' @param df A data frame containing the text data to be translated.
#' @param column A character string specifying the column name to be translated.
#' @param source_lang A character string indicating the source language code (e.g., `"ca"` for Catalan).
#'   Set `"auto"` to automatically detect the language of each row.
#' @param target_lang A character string indicating the target language code (e.g., `"en"` for English).
#' @param file_path A character string specifying the CSV file path to save the results.
#' @param batch_size An integer specifying how many rows to process per batch.
#' @param max_cores An integer specifying the number of CPU cores to use for parallel processing.
#' @param context (Optional) A string providing additional context to prepend to the text before translation.
#'                The context is separated from the original text using `" ||| "` and removed after translation.
#'                This is useful for improving the accuracy of short text translations.
#' @param id_column (Optional) A character string specifying the name of a column
#'                  in `df` that contains unique row identifiers. If `NULL`,
#'                  row numbers will be used as a temporary ID to preserve order.
#' @param service A character string specifying the translation service to use.
#'                Options are `"apertium"` (rule-based translations) or
#'                `"libretranslate"` (neural machine translations).
#'                Default is `"libretranslate"`.
#'                Ensure the corresponding service is running before translation.
#'
#' @return None (writes results directly to a CSV file and prints a summary with a progress bar).
#'
#' @details
#' The function communicates with locally running instances of LibreTranslate or Apertium.
#' Ensure that LibreTranslate is running on port 5000, or Apertium on port 2737, before using this function.
#'
#' @import httr jsonlite data.table parallel
#' @export
#'
#' @examples
#' course_details_df <- data.frame(
#' id = 1:5,
#' course_name = c(
#'   "Geografia i territori",
#'   "Història de l'art modern",
#'   "Fonaments de química",
#'   "Anàlisi matemàtica",
#'   "Dret constitucional"
#' ),
#' stringsAsFactors = FALSE  # Ensure text remains as character type
#' )
#'
#' # Translate and write directly to CSV with progress tracking
#' translate_column(course_details_df, column = "course_name", source_lang = "ca", target_lang = "en", file_path = "course_name-en.csv", max_cores = 4)
#'
#' # Translate with context for short texts
#' translate_column(df = course_details_df, column = "course_name",
#'                  source_lang = "ca", target_lang = "en",
#'                  file_path = "course_name-en.csv", max_cores = 4,
#'                  context = "Aquest és el nom de l'assignatura:")


translate_column <- function(df, column, source_lang = "auto", target_lang = "en",
                             file_path = "output.csv", batch_size = 100, max_cores = 4,
                             context = NULL, id_column = NULL, service = "libretranslate") {

  library(parallel)
  library(pbapply)

  if (!column %in% colnames(df)) {
    stop(paste("Error: The column", column, "does not exist in the provided data frame."))
  }

  if (!file.exists(file_path)) {
    data.table::fwrite(data.frame(id = character(), original = character(), detected_language = character(), confidence = numeric(), translated_text = character()),
           file = file_path, append = FALSE)
  }

  df[[column]] <- tolower(df[[column]])

  if (!is.null(context)) {
    df[[column]] <- paste0(context, df[[column]])
  }

  if (is.null(id_column)) {
    df$id_temp <- seq_len(nrow(df))
    id_column <- "id_temp"
  } else if (!id_column %in% colnames(df)) {
    stop("Error: The specified ID column does not exist in the data frame.")
  }

    # Function to convert language codes to Apertium format
  convert_lang_code <- function(lang, service) {
    if (service == "apertium") {
      lang_map <- list(
        "ca" = "cat",
        "es" = "spa",
        "en" = "eng",
        "fr" = "fra",
        "de" = "deu"
      )
      return(ifelse(lang %in% names(lang_map), lang_map[[lang]], lang)) # Convert if exists, else keep original
    }
    return(lang) # If using LibreTranslate, keep original code
  }

  detect_language_libretranslate <- function(text) {
    url <- "http://localhost:5000/detect"
    response <- tryCatch({
      httr::POST(url, body = list(q = text), encode = "json", httr::accept_json())
    }, error = function(e) {
      return(list(detectedLanguage = NA, confidence = NA))
    })

    if (!is.null(response) && httr::status_code(response) == 200) {
      json <- jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
      return(list(detectedLanguage = json$language[1], confidence = json$confidence[1]))
    } else {
      return(list(detectedLanguage = NA, confidence = NA))
    }
  }

  detect_language_apertium <- function(text) {
    url <- "http://localhost:2737/identifyLang"

    response <- tryCatch({
      httr::GET(url, query = list(q = text))
    }, error = function(e) {
      message("Error in request: ", e$message)
      return(list(detectedLanguage = NA, confidence = NA))
    })

    # Ensure response is valid
    if (is.null(response) || !inherits(response, "response") || httr::status_code(response) != 200) {
      message("Invalid HTTP response")
      return(list(detectedLanguage = NA, confidence = NA))
    }

    # Parse JSON response safely
    content <- tryCatch({
      jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
    }, error = function(e) {
      message("Error parsing JSON: ", e$message)
      return(list(detectedLanguage = NA, confidence = NA))
    })

    # Ensure response contains valid language data
    if (length(content) == 0 || !is.list(content)) {
      message("Empty or malformed response from Apertium")
      return(list(detectedLanguage = NA, confidence = NA))
    }

    # Convert JSON object to named vector
    lang_conf <- unlist(content)

    # Find the language with the highest confidence
    if (length(lang_conf) > 0) {
      best_lang <- names(lang_conf)[which.max(lang_conf)]
      best_confidence <- max(lang_conf)
    } else {
      best_lang <- NA
      best_confidence <- NA
    }

    return(list(detectedLanguage = best_lang, confidence = best_confidence))
  }



  detect_language <- function(text, service) {
    if (missing(service)) {
      stop("Error: 'service' argument is missing.")
    }

    if (service == "libretranslate") {
      return(detect_language_libretranslate(text))
    } else if (service == "apertium") {
      return(detect_language_apertium(text))
    } else {
      stop("Invalid service. Choose either 'libretranslate' or 'apertium'.")
    }
  }

  translate_text_libretranslate <- function(text, source_lang, target_lang) {
    url <- "http://localhost:5000/translate"
    response <- tryCatch({
      httr::POST(url, body = list(q = text, source = source_lang, target = target_lang, format = "text"), encode = "json", httr::accept_json())
    }, error = function(e) {
      return("Error in translation request")
    })

    if (!is.null(response) && httr::status_code(response) == 200) {
      json <- jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
      return(json$translatedText)
    } else {
      return("Error in translation request")
    }
  }


  # Updated translation function for Apertium
  translate_text_apertium <- function(text, source_lang, target_lang) {
    url <- "http://localhost:2737/translate"

    # Convert language codes if using Apertium
    source_lang <- convert_lang_code(source_lang, "apertium")
    target_lang <- convert_lang_code(target_lang, "apertium")

    response <- tryCatch({
      httr::GET(url, query = list(
        q = text,
        langpair = paste(source_lang, target_lang, sep = "|")
      ))
    }, error = function(e) {
      return(NULL) # Return NULL if the request fails
    })

    if (!is.null(response) && httr::status_code(response) == 200) {
      json <- jsonlite::fromJSON(httr::content(response, "text", encoding = "UTF-8"))
      return(json$responseData$translatedText)
    } else {
      return("Translation Error")
    }
  }



  translate_text <- function(text, source_lang, target_lang, service) {
    if (service == "libretranslate") {
      return(translate_text_libretranslate(text, source_lang, target_lang))
    } else if (service == "apertium") {
      return(translate_text_apertium(text, source_lang, target_lang))
    } else {
      stop("Invalid translation service. Choose either 'libretranslate' or 'apertium'.")
    }
  }

  num_cores <- min(max_cores, detectCores() - 1)
  closeAllConnections()
  cl <- parallel::makeCluster(num_cores)

  parallel::clusterEvalQ(cl, {
    library(httr)
    library(jsonlite)
    library(data.table)
  })

  # Explicitly pass service and other variables to the workers
  parallel::clusterExport(cl, varlist = c(
    "detect_language", "detect_language_libretranslate", "detect_language_apertium",
    "translate_text", "translate_text_libretranslate", "translate_text_apertium",
    "convert_lang_code", "source_lang", "target_lang", "file_path", "service"
  ), envir = environment())


  batches <- split(df[, c(id_column, column)], ceiling(seq_along(df[[column]]) / batch_size))
  num_batches <- length(batches)

  cat(sprintf("Processing %d batches using %d cores with %s...", num_batches, num_cores, service))

  process_and_write_batch <- function(batch, file_path) {
    results <- lapply(seq_along(batch[[column]]), function(i) {
      text <- batch[[column]][i]
      row_id <- batch[[id_column]][i]

      if (is.null(text) || is.na(text) || text == "" || trimws(text) == "") {
        return(data.frame(id = row_id, original = NA, detected_language = NA, confidence = NA, translated_text = NA, stringsAsFactors = FALSE))
      }

      detected <- detect_language(text,service)
      final_source_lang <- if (source_lang == "auto") detected$detectedLanguage else source_lang

      # Convert to Apertium format if necessary
      if (service == "apertium") {
        final_source_lang <- convert_lang_code(final_source_lang, service)
      }

      translated_text <- translate_text(text, final_source_lang, target_lang, service)

      if (!is.null(context)) {
        pattern <- "^\\s*[^:]+:\\s*"
        text <- sub(pattern, "", text)
        translated_text <- sub(pattern, "", translated_text)
      }

      return(data.frame(
        id = row_id,
        original = text,
        detected_language = detected$detectedLanguage,
        confidence = detected$confidence,
        translated_text = translated_text,
        stringsAsFactors = FALSE
      ))
    })

    data.table::fwrite(do.call(rbind, results), file = file_path, append = TRUE, col.names = !file.exists(file_path))
  }

  pbapply::pblapply(batches, function(batch) {
    process_and_write_batch(batch, file_path)
  }, cl = cl)

  parallel::stopCluster(cl)
  closeAllConnections()

  cat("Translation process completed.")
}
