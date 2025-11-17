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

#'
#' @return None (writes results directly to a CSV file and prints a summary with a progress bar).
#'
#' @details
#' The function communicates with a locally running instance of LibreTranslate. 
#' Ensure that LibreTranslate is running on port 5000 before using this function.
#'
#' To start LibreTranslate using Docker:
#' ```
#' docker run -p 5000:5000 libretranslate/libretranslate
#' ```
#' Alternatively, using Docker Compose:
#' ```
#' docker compose -f ./docker/docker-compose-libretranslate.yml up
#' ```
#'
#' @import httr jsonlite data.table parallel progressr
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
                             context = NULL, id_column = NULL) {
  library(parallel)
  library(pbapply)  # Provides progress bars for parallel tasks
  
  cat("Starting translation process...\n")
  start_time <- Sys.time()
  
  if (!column %in% colnames(df)) {
    stop(paste("Error: The column", column, "does not exist in the provided data frame."))
  }
  
  if (!file.exists(file_path)) {
    fwrite(data.frame(id = character(), original = character(), detected_language = character(), confidence = numeric(), translated_text = character()), 
           file = file_path, append = FALSE)
  }
  
  # Avoid capitalized text as it tents to make errors in translation
  df[[column]] <-   tolower(df[[column]])
  
  if (!is.null(context)) {
    df[[column]] <- paste0(context, df[[column]])
  }
  
  if (is.null(id_column)) {
    df$id_temp <- seq_len(nrow(df))  # Create a temporary ID if none is provided
    id_column <- "id_temp"
  } else if (!id_column %in% colnames(df)) {
    stop("Error: The specified ID column does not exist in the data frame.")
  }
  
  detect_language <- function(text) {
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
  
  libretranslate_translate <- function(text, source_lang, target_lang) {
    url <- "http://localhost:5000/translate"
    response <- tryCatch({
      httr::POST(url, body = list(
        q = text,
        source = source_lang,
        target = target_lang,
        format = "text"
      ), encode = "json", httr::accept_json())
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
  
  num_cores <- min(max_cores, detectCores() - 1)
  closeAllConnections()
  cl <- makeCluster(num_cores)
  
  clusterEvalQ(cl, {
    library(httr)
    library(jsonlite)
    library(data.table)
  })
  
  clusterExport(cl, varlist = c("detect_language", "libretranslate_translate", "source_lang", "target_lang", "file_path"), envir = environment())
  
  # Split data into batches
  batches <- split(df[, c(id_column, column)], ceiling(seq_along(df[[column]]) / batch_size))
  num_batches <- length(batches)
  
  cat(sprintf("Processing %d batches using %d cores...\n", num_batches, num_cores))
  
  process_and_write_batch <- function(batch, file_path) {
    results <- lapply(seq_along(batch[[column]]), function(i) {
      text <- batch[[column]][i]
      row_id <- batch[[id_column]][i]  # Always store the ID
      
      if (is.null(text) || is.na(text) || text == "" || trimws(text) == "") {
        return(data.frame(id = row_id, original = NA, detected_language = NA, confidence = NA, translated_text = NA, stringsAsFactors = FALSE))
      }
      
      
      detected <- detect_language(text)
      final_source_lang <- if (source_lang == "auto") detected$detectedLanguage else source_lang
      translated_text <- libretranslate_translate(text, final_source_lang, target_lang)
      
      # Remove the prepended context *only if context was used*
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
    
    fwrite(do.call(rbind, results), file = file_path, append = TRUE, col.names = !file.exists(file_path))
  }
  
  # Use `pblapply()` to correctly update progress bar
  pbapply::pblapply(batches, function(batch) {
    process_and_write_batch(batch, file_path)
  }, cl = cl)
  
  stopCluster(cl)
  closeAllConnections()
  
  cat("Translation process completed in", round(difftime(Sys.time(), start_time, units = "mins"), 2), "minutes.\n")
}

