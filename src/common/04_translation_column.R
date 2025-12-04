# File: src/common/04_translation_column.R
# Purpose:
#   High-level helper to translate a single column from a data frame and append
#   results to a CSV file.
#
# Responsibilities:
#   - Validate inputs (column and id column)
#   - Initialise the output CSV with a stable schema
#   - Batch rows and call the translation client (translate_text / detect_language)
#   - Apply the tolerant fallback strategy (translate_tolerant)
#
# Notes:
#   - This function does not create/close clusters. Parallelism should be handled
#     by the caller (e.g. {targets}).
#   - Service endpoints and HTTP calls are implemented in 04_translation_service.R
#   - Tolerant fallback splitting is implemented in 04_translation_tolerant.R

translation_results_schema <- function() {
  data.frame(
    id                     = character(),
    original               = character(),
    detected_language      = character(),
    confidence             = numeric(),
    translated_text        = character(),
    difficulty             = character(),
    split_strategy         = character(),
    parts_total            = integer(),
    failed_parts           = integer(),
    failed_parts_ratio     = numeric(),
    sentences_total        = integer(),
    failed_sentences       = integer(),
    failed_sentences_ratio = numeric(),
    stringsAsFactors       = FALSE
  )
}

translation_init_output_file <- function(file_path) {
  if (!file.exists(file_path)) {
    data.table::fwrite(
      translation_results_schema(),
      file   = file_path,
      append = FALSE
    )
  }
}

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
    stop(
      "translate_column(): column '", column,
      "' does not exist in the provided data frame.",
      call. = FALSE
    )
  }
  
  # Initialise output schema if it does not exist.
  translation_init_output_file(file_path)
  
  # We just ensured a header exists, so always append without headers.
  append_with_header <- FALSE
  
  # Stable ID column.
  if (is.null(id_column)) {
    df$id_temp <- seq_len(nrow(df))
    id_column  <- "id_temp"
  } else if (!id_column %in% colnames(df)) {
    stop(
      "translate_column(): specified id_column '", id_column,
      "' does not exist in the data frame.",
      call. = FALSE
    )
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
          id                     = row_id,
          original               = NA_character_,
          detected_language      = NA_character_,
          confidence             = NA_real_,
          translated_text        = NA_character_,
          difficulty             = NA_character_,
          split_strategy         = NA_character_,
          parts_total            = NA_integer_,
          failed_parts           = NA_integer_,
          failed_parts_ratio     = NA_real_,
          sentences_total        = NA_integer_,
          failed_sentences       = NA_integer_,
          failed_sentences_ratio = NA_real_,
          stringsAsFactors       = FALSE
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
      
      # Detect only if needed
      detected <- if (source_lang == "auto") {
        detect_language(text_for_translate, service)
      } else {
        list(detectedLanguage = NA_character_, confidence = NA_real_)
      }
      
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
            translate_fun = translate_text,
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
        id                     = row_id,
        original               = text,  # original text without prefix/suffix
        detected_language      = detected$detectedLanguage,
        confidence             = detected$confidence,
        translated_text        = translated_text,
        difficulty             = difficulty,
        split_strategy         = split_strategy,
        parts_total            = parts_total,
        failed_parts           = failed_parts,
        failed_parts_ratio     = failed_parts_ratio,
        sentences_total        = sentences_total,
        failed_sentences       = failed_sentences,
        failed_sentences_ratio = failed_sentences_ratio,
        stringsAsFactors       = FALSE
      )
    })
    
    results_df <- do.call(rbind, results)
    
    # Append, always without headers (file already initialised).
    data.table::fwrite(
      results_df,
      file      = file_path,
      append    = TRUE,
      col.names = append_with_header
    )
  }
  
  message("translate_column(): translation process completed.")
  invisible(NULL)
}
