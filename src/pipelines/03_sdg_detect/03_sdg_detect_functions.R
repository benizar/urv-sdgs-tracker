# File: src/pipelines/03_detect_sdg/03_detect_sdg_functions.R
#
# SDG detection helpers for the URV SDGs tracker pipeline.
#
# Responsibilities:
#   - build the SDG input table (document_number x section x text)
#     from the translated guides table (guides_translated)
#   - run text2sdg (systems or ensemble)
#   - summarise hits (long and wide)
#   - attach SDG summaries back to the guides table
#   - build reviewer-friendly outputs:
#       * guides_sdg_summary: one row per course + summary fields
#       * guides_sdg_review: one row per detected SDG (course repeats)
#
# NOTE:
#   - guides_translated is produced by the translation phase, which
#     can run in "auto" or "reviewer" mode. From the SDG point of view,
#     it is always just a data frame with *_en columns.
#   - attach_sdg_to_guides() only joins sdg_hits_wide back to whatever
#     guides table you pass in (typically guides_translated).

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

sdg_to_id <- function(x) {
  # Accepts "1" / "01" / "SDG-01" / etc and returns "SDG-01"
  num <- suppressWarnings(as.integer(stringr::str_extract(as.character(x), "\\d+")))
  ifelse(is.na(num), NA_character_, sprintf("SDG-%02d", num))
}

collapse_sorted_unique <- function(x, sep = ", ") {
  x <- unique(stats::na.omit(as.character(x)))
  x <- x[nzchar(x)]
  if (!length(x)) "" else paste(sort(x), collapse = sep)
}

# -------------------------------------------------------------------
# SDG labels (external resource)
# -------------------------------------------------------------------
# Reads a CSV with SDG labels in multiple languages to avoid hardcoding.
#
# Expected minimum column:
#   - sdg_id (e.g. "SDG-01")
#
# Recommended columns (any subset is fine):
#   - label_ca, label_es, label_en, ...
# Optionally:
#   - sdg_num (1..17) (not required)
#
# The function returns a tibble with:
#   sdg_id + ods_label_* columns (renamed from label_*)
read_sdg_labels <- function(path = "resources/sdg_labels.csv") {
  if (!file.exists(path)) {
    stop("read_sdg_labels(): SDG labels file not found: ", path, call. = FALSE)
  }
  
  # Try ';' first (common in this project), then ',' as fallback.
  df <- tryCatch(
    readr::read_delim(path, delim = ";", show_col_types = FALSE, progress = FALSE),
    error = function(e) NULL
  )
  if (is.null(df)) {
    df <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE)
  }
  
  if (!"sdg_id" %in% names(df)) {
    stop("read_sdg_labels(): labels CSV must include a 'sdg_id' column.", call. = FALSE)
  }
  
  df <- df |>
    dplyr::mutate(sdg_id = as.character(sdg_id)) |>
    dplyr::filter(!is.na(sdg_id) & nzchar(sdg_id))
  
  # Normalize labels naming: label_xx -> ods_label_xx
  label_cols <- grep("^label_[a-z]{2,}$", names(df), value = TRUE)
  if (length(label_cols)) {
    df <- df |>
      dplyr::rename_with(
        .fn = function(nm) sub("^label_", "ods_label_", nm),
        .cols = dplyr::all_of(label_cols)
      )
  }
  
  # Keep stable join key, plus any language labels (already normalized),
  # plus any auxiliary columns you may want later (e.g., sdg_num).
  keep_cols <- c(
    "sdg_id",
    intersect(c("sdg_num"), names(df)),
    grep("^ods_label_[a-z]{2,}$", names(df), value = TRUE)
  )
  keep_cols <- unique(keep_cols)
  df <- df |>
    dplyr::select(dplyr::all_of(keep_cols)) |>
    dplyr::distinct(sdg_id, .keep_all = TRUE)
  
  df
}

# -------------------------------------------------------------------
# Build SDG input table from a translated guides table and sdg config
# -------------------------------------------------------------------

build_sdg_input <- function(guides_translated, sdg_cfg) {
  combine_groups <- sdg_cfg$combine_groups
  
  if (is.null(combine_groups) || !length(combine_groups)) {
    stop(
      "build_sdg_input(): missing `combine_groups:` in config/sdg_detection.yml.\n\n",
      "Please define at least one group, e.g.:\n",
      "combine_groups:\n",
      "  - id: \"course_info\"\n",
      "    prefix: \"Course information: \"\n",
      "   columns:\n",
      "     - course_description_en\n",
      "     - course_contents_en\n",
      call. = FALSE
    )
  }
  
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_sdg_input(): `guides_translated` must contain `document_number`.", call. = FALSE)
  }
  
  out_list <- list()
  
  for (grp in combine_groups) {
    id     <- grp$id
    prefix <- grp$prefix %||% ""
    cols   <- grp$columns %||% character(0)
    
    if (is.null(id) || !nzchar(id)) {
      stop(
        "build_sdg_input(): combine_groups entries must include a non-empty 'id' field.",
        call. = FALSE
      )
    }
    
    if (!length(cols)) {
      stop(
        "build_sdg_input(): combine_groups entry '", id, "' has no columns.",
        call. = FALSE
      )
    }
    
    cols_existing <- intersect(cols, names(guides_translated))
    missing_cols  <- setdiff(cols, cols_existing)
    
    if (length(missing_cols)) {
      stop(
        "build_sdg_input(): combine_groups '", id,
        "' refers to missing columns: ",
        paste(missing_cols, collapse = ", "),
        call. = FALSE
      )
    }
    
    tmp <- guides_translated |>
      dplyr::select(document_number, dplyr::all_of(cols_existing))
    
    text_vec <- apply(
      tmp[, cols_existing, drop = FALSE],
      1,
      function(row) {
        pieces <- trimws(as.character(row))
        pieces <- pieces[!is.na(pieces) & pieces != ""]
        if (!length(pieces)) return(NA_character_)
        paste(pieces, collapse = " ||| ")
      }
    )
    
    if (nzchar(prefix)) {
      text_vec <- ifelse(
        is.na(text_vec) | text_vec == "",
        NA_character_,
        paste0(prefix, text_vec)
      )
    }
    
    df_grp <- tibble::tibble(
      document_number = guides_translated$document_number,
      section         = id,
      text            = text_vec
    ) |>
      dplyr::filter(!is.na(text) & text != "")
    
    out_list[[id]] <- df_grp
  }
  
  sdg_input <- dplyr::bind_rows(out_list)
  
  if (!nrow(sdg_input)) {
    stop(
      "build_sdg_input(): no non-empty text found for any combine_groups. ",
      "Check that the *_en columns referenced in combine_groups exist and are not all empty.",
      call. = FALSE
    )
  }
  
  sdg_input
}

# -------------------------------------------------------------------
# Run text2sdg with configurable options
# -------------------------------------------------------------------

run_text2sdg_detection <- function(sdg_input, sdg_cfg) {
  method  <- sdg_cfg$method  %||% "systems"
  sdgs    <- sdg_cfg$sdgs    %||% 1:17
  verbose <- sdg_cfg$verbose %||% FALSE
  
  method <- tolower(method)
  
  if (!nrow(sdg_input)) {
    return(
      tibble::tibble(
        document_number = character(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        document        = integer(0)
      )
    )
  }
  
  if (identical(method, "systems")) {
    systems <- sdg_cfg$systems %||% c("Aurora", "Elsevier", "Auckland", "SIRIS")
    output  <- sdg_cfg$output  %||% "documents"
    
    hits <- text2sdg::detect_sdg_systems(
      text    = sdg_input$text,
      system  = systems,
      sdgs    = sdgs,
      output  = output,
      verbose = verbose
    )
    
  } else if (identical(method, "ensemble")) {
    hits <- text2sdg::detect_sdg(
      text    = sdg_input$text,
      sdgs    = sdgs,
      verbose = verbose
    )
    
    # Ensure a 'system' column exists downstream
    if (!"system" %in% names(hits)) {
      hits$system <- "Ensemble"
    }
    
  } else {
    stop("run_text2sdg_detection(): unknown method in config: ", method, call. = FALSE)
  }
  
  if (!nrow(hits)) {
    message("run_text2sdg_detection(): text2sdg returned 0 hits.")
    hits$document_number <- character(0)
    hits$section         <- character(0)
    return(hits)
  }
  
  if (!"document" %in% names(hits)) {
    stop("run_text2sdg_detection(): text2sdg output does not include 'document' column.", call. = FALSE)
  }
  
  doc_index <- as.integer(hits$document)
  doc_map   <- sdg_input$document_number
  sec_map   <- sdg_input$section
  
  if (any(is.na(doc_index)) || any(doc_index < 1L) || any(doc_index > length(doc_map))) {
    stop("run_text2sdg_detection(): invalid 'document' indices in hits.", call. = FALSE)
  }
  
  hits |>
    dplyr::mutate(
      document_number = doc_map[doc_index],
      section         = sec_map[doc_index],
      sdg             = as.character(sdg)
    )
}

# -------------------------------------------------------------------
# Long summary: document_number x section x system x sdg
# -------------------------------------------------------------------

summarise_sdg_hits_long <- function(hits) {
  if (!nrow(hits)) {
    return(
      tibble::tibble(
        document_number = character(0),
        section         = character(0),
        system          = character(0),
        sdg             = character(0),
        n_hits          = integer(0),
        sdg_num         = character(0)
      )
    )
  }
  
  if (!"document_number" %in% names(hits)) stop("summarise_sdg_hits_long(): 'document_number' is required.", call. = FALSE)
  if (!"section" %in% names(hits))         stop("summarise_sdg_hits_long(): 'section' is required.", call. = FALSE)
  if (!"system" %in% names(hits))          stop("summarise_sdg_hits_long(): 'system' is required.", call. = FALSE)
  if (!"sdg" %in% names(hits))             stop("summarise_sdg_hits_long(): 'sdg' is required.", call. = FALSE)
  
  hits |>
    dplyr::mutate(sdg = as.character(sdg)) |>
    dplyr::group_by(document_number, section, system, sdg) |>
    dplyr::summarise(
      n_hits  = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::mutate(sdg_num = stringr::str_extract(sdg, "\\d+"))
}

# -------------------------------------------------------------------
# Wide summary: 1 row per document_number, 1 col per (section, system, sdg)
# -------------------------------------------------------------------

summarise_sdg_hits_wide <- function(sdg_long, sdg_cfg) {
  if (!nrow(sdg_long)) {
    return(tibble::tibble(document_number = character(0)))
  }
  
  agg_mode <- sdg_cfg$aggregate$mode     %||% "counts"
  min_hits <- sdg_cfg$aggregate$min_hits %||% 1L
  
  sdg_long |>
    dplyr::mutate(
      value = if (identical(agg_mode, "binary")) as.integer(n_hits >= min_hits) else n_hits,
      colname = paste0("sdg_", sdg_num, "_", system, "_", section)
    ) |>
    dplyr::select(document_number, colname, value) |>
    tidyr::pivot_wider(
      id_cols     = document_number,
      names_from  = colname,
      values_from = value,
      values_fill = 0
    )
}

# -------------------------------------------------------------------
# Attach SDG wide summary back to the guides table
# -------------------------------------------------------------------

attach_sdg_to_guides <- function(guides_translated, sdg_hits_wide) {
  guides_translated |>
    dplyr::left_join(sdg_hits_wide, by = "document_number")
}

# -------------------------------------------------------------------
# guides_sdg_summary (1 row per course)
# -------------------------------------------------------------------

build_guides_sdg_summary <- function(guides_translated, sdg_hits_long, sdg_cfg) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_summary(): guides_translated must contain document_number.", call. = FALSE)
  }
  
  # Base: all course fields (already translated)
  out <- guides_translated
  
  if (!nrow(sdg_hits_long)) {
    # Add empty summary columns (stable schema)
    out <- out |>
      dplyr::mutate(
        sdg_any = FALSE,
        sdg_n_distinct = 0L,
        sdg_sdgs = "",
        sdg_n_systems = 0L,
        sdg_systems = "",
        sdg_total_hits = 0L
      )
    return(out)
  }
  
  # Global per course
  global <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number) |>
    dplyr::summarise(
      sdg_n_distinct = dplyr::n_distinct(sdg_id),
      sdg_sdgs       = collapse_sorted_unique(sdg_id),
      sdg_n_systems  = dplyr::n_distinct(system),
      sdg_systems    = collapse_sorted_unique(system),
      sdg_total_hits = sum(n_hits, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(sdg_any = sdg_n_distinct > 0L) |>
    dplyr::relocate(sdg_any, .before = sdg_n_distinct)
  
  # Per section per course
  by_section <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number, section) |>
    dplyr::summarise(
      n_distinct = dplyr::n_distinct(sdg_id),
      sdgs       = collapse_sorted_unique(sdg_id),
      n_systems  = dplyr::n_distinct(system),
      systems    = collapse_sorted_unique(system),
      total_hits = sum(n_hits, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      id_cols = document_number,
      names_from = section,
      values_from = c(n_distinct, sdgs, n_systems, systems, total_hits),
      names_glue = "sdg_{.value}_{section}"
    )
  
  out |>
    dplyr::left_join(global, by = "document_number") |>
    dplyr::left_join(by_section, by = "document_number") |>
    dplyr::mutate(
      sdg_any        = dplyr::coalesce(sdg_any, FALSE),
      sdg_n_distinct = dplyr::coalesce(as.integer(sdg_n_distinct), 0L),
      sdg_sdgs       = dplyr::coalesce(sdg_sdgs, ""),
      sdg_n_systems  = dplyr::coalesce(as.integer(sdg_n_systems), 0L),
      sdg_systems    = dplyr::coalesce(sdg_systems, ""),
      sdg_total_hits = dplyr::coalesce(as.integer(sdg_total_hits), 0L)
    )
}

# -------------------------------------------------------------------
# guides_sdg_review (1 row per detected SDG)
# -------------------------------------------------------------------

build_guides_sdg_review <- function(guides_translated, sdg_hits_long, sdg_cfg) {
  if (!"document_number" %in% names(guides_translated)) {
    stop("build_guides_sdg_review(): guides_translated must contain document_number.", call. = FALSE)
  }
  
  if (!nrow(sdg_hits_long)) {
    return(
      tibble::tibble(
        document_number = character(0),
        sdg_id = character(0),
        ods_label_ca = character(0),
        total_hits = integer(0),
        n_sections = integer(0),
        sections = character(0),
        n_systems = integer(0),
        systems = character(0)
      )
    )
  }
  
  # Aggregate doc + SDG
  base <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number, sdg_id) |>
    dplyr::summarise(
      total_hits = sum(n_hits, na.rm = TRUE),
      n_sections = dplyr::n_distinct(section),
      sections   = collapse_sorted_unique(section),
      n_systems  = dplyr::n_distinct(system),
      systems    = collapse_sorted_unique(system),
      .groups = "drop"
    )
  
  # Per section details (hits + systems) for this doc+sdg
  by_sec <- sdg_hits_long |>
    dplyr::mutate(sdg_id = sdg_to_id(sdg)) |>
    dplyr::group_by(document_number, sdg_id, section) |>
    dplyr::summarise(
      hits_section    = sum(n_hits, na.rm = TRUE),
      systems_section = collapse_sorted_unique(system),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      id_cols = c(document_number, sdg_id),
      names_from = section,
      values_from = c(hits_section, systems_section),
      names_glue = "{.value}_{section}"
    )
  
  # Labels (external resource; optional but useful for Excel review)
  labels_path <- sdg_cfg$labels_csv %||% "resources/sdg_labels.csv"
  labels_tbl  <- tryCatch(read_sdg_labels(labels_path), error = function(e) NULL)
  
  # Lengths (per translated fields) help later stratified sampling (you’ll do it in another target)
  # Use what exists; don’t fail if a column is absent.
  get_len <- function(df, col) {
    if (!col %in% names(df)) return(rep(0L, nrow(df)))
    stringr::str_length(dplyr::coalesce(as.character(df[[col]]), ""))
  }
  
  meta <- guides_translated |>
    dplyr::mutate(
      course_description_len = get_len(dplyr::pick(dplyr::everything()), "course_description_en"),
      course_contents_len    = get_len(dplyr::pick(dplyr::everything()), "course_contents_en"),
      course_competences_len = get_len(dplyr::pick(dplyr::everything()), "course_competences_and_results_en"),
      course_references_len  = get_len(dplyr::pick(dplyr::everything()), "course_references_en"),
      total_translated_len   = course_description_len + course_contents_len + course_competences_len + course_references_len
    )
  
  # Join: one row per detected SDG
  out <- base |>
    dplyr::left_join(by_sec, by = c("document_number", "sdg_id"))
  
  if (!is.null(labels_tbl) && nrow(labels_tbl)) {
    out <- out |>
      dplyr::left_join(labels_tbl, by = "sdg_id")
    
    # Keep compatibility with the previous column name if the CSV uses 'ods_label_ca'
    if (!"ods_label_ca" %in% names(out) && "ods_label_ca" %in% names(labels_tbl)) {
      # no-op (already would exist); kept for clarity
    }
  }
  
  out <- out |>
    dplyr::inner_join(meta, by = "document_number")
  
  # Keep the previous layout: put Catalan label right after sdg_id when present.
  if ("ods_label_ca" %in% names(out)) {
    out <- out |>
      dplyr::relocate(ods_label_ca, .after = sdg_id)
  }
  
  out
}
