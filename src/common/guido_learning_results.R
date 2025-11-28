extract_guido_learning_results_from_html <- function(html_text,
                                                     collapse = ";\n",
                                                     use_cache = TRUE) {
  # Extract GUIdO learning results as an ordered, de-duplicated list:
  # - Keep parent competences (SE1 - ...) without duplicating child items.
  # - Keep leaf learning outcomes (SE1.5 - ...) once.
  # - Preserve order and join with `collapse`.
  
  parse_one <- function(x) {
    if (is.na(x) || !nzchar(x)) return(NA_character_)
    
    # Wrap to ensure a single root node for HTML parsing.
    doc <- xml2::read_html(paste0("<div>", x, "</div>"))
    
    # Parent nodes: take ONLY direct text nodes (avoid dragging nested <li> text).
    parent_nodes <- xml2::xml_find_all(
      doc,
      ".//li[contains(@class,'li_competencia')] | .//li[contains(@class,'li_competencia_especifica')]"
    )
    
    parent_txt <- vapply(parent_nodes, function(li) {
      tn <- xml2::xml_find_all(li, "./text()")
      txt <- paste(xml2::xml_text(tn), collapse = " ")
      txt <- gsub("[ \t\r\n]+", " ", txt, perl = TRUE)
      trimws(txt)
    }, character(1))
    
    parent_txt <- parent_txt[nzchar(parent_txt)]
    
    # Leaf items: learning outcomes that do not contain other <li> descendants.
    leaf_nodes <- xml2::xml_find_all(doc, ".//li[not(descendant::li)]")
    leaf_txt <- xml2::xml_text(leaf_nodes)
    leaf_txt <- gsub("[ \t\r\n]+", " ", leaf_txt, perl = TRUE)
    leaf_txt <- trimws(leaf_txt)
    leaf_txt <- leaf_txt[nzchar(leaf_txt)]
    
    items <- c(parent_txt, leaf_txt)
    
    # Drop duplicates whilst preserving the first occurrence.
    items <- items[!duplicated(items)]
    
    if (!length(items)) return(NA_character_)
    paste(items, collapse = collapse)
  }
  
  if (use_cache) {
    uniq <- unique(html_text)
    idx  <- match(html_text, uniq)
    uniq_out <- vapply(uniq, parse_one, character(1))
    return(uniq_out[idx])
  }
  
  vapply(html_text, parse_one, character(1))
}
