# File: src/common/05_urls.R
# URL parsers for GUIdO and DOCnet course URLs.

# Utility: right-trim slashes from URLs
rtrim_slash <- function(x) {
  sub("/+$", "", x)
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
  rtrim_slash(base)
}

# Returns named lists to be expanded with tidyr::unnest_wider().
# DOCnet keeps optional modality suffix letter in course_code (e.g., 12815106v).

parse_guido_course_url <- function(url) {
  url <- as.character(url)
  if (is.na(url) || !nzchar(url)) {
    return(list(
      centre_code = NA_character_,
      programme_code = NA_character_,
      course_code = NA_character_
    ))
  }
  
  centre_code    <- stringr::str_match(url, "/centres/(\\d{3})/")[, 2]
  programme_code <- stringr::str_match(url, "/ensenyaments/(\\d+)/")[, 2]
  course_code    <- stringr::str_match(url, "/assignatures/(\\d+)/")[, 2]
  
  list(
    centre_code = centre_code,
    programme_code = programme_code,
    course_code = course_code
  )
}

parse_docnet_course_url <- function(url) {
  url <- as.character(url)
  if (is.na(url) || !nzchar(url)) {
    return(list(
      centre_code = NA_character_,
      programme_code = NA_character_,
      course_code = NA_character_,
      modality = NA_character_
    ))
  }
  
  centre_code    <- stringr::str_match(url, "centre=([0-9]+)")[, 2]
  programme_code <- stringr::str_match(url, "ensenyament=([0-9]+)")[, 2]
  # optional trailing letter (v/s/p/...)
  course_code    <- stringr::str_match(url, "assignatura=([0-9]+[a-z]?)")[, 2]
  
  modality <- if (!is.na(course_code) && nzchar(course_code)) {
    m <- stringr::str_match(course_code, "([a-z])$")[, 2]
    ifelse(is.na(m), NA_character_, m)
  } else {
    NA_character_
  }
  
  list(
    centre_code = centre_code,
    programme_code = programme_code,
    course_code = course_code,
    modality = modality
  )
}
