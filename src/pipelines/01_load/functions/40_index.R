# -------------------------------------------------------------------
# High-level entry point: build per-course index from loaded tables.
# (Step version: course master + URL normalization for DOCnet)
# -------------------------------------------------------------------

build_guides_index_from_loaded <- function(centres_list,
                                           programmes_list,
                                           course_details_list,
                                           guido_docnet_course_code_map,
                                           docnet_course_info,
                                           guido_course_info) {
  # 1) Base table (course details) + labels + mapping + canonical course_code + canonical course_url
  out <- course_details_list |>
    dplyr::filter(source_system != "External") |>
    dplyr::left_join(
      programmes_list |>
        dplyr::select(dplyr::all_of(c("centre_programme_code", "programme_name"))),
      by = "centre_programme_code"
    ) |>
    dplyr::left_join(
      centres_list |>
        dplyr::select(dplyr::all_of(c("centre_code", "centre_name"))),
      by = "centre_code"
    ) |>
    dplyr::left_join(
      guido_docnet_course_code_map |>
        dplyr::select(dplyr::all_of(c(
          "guido_centre_programme_course_code",
          "docnet_centre_programme_course_code",
          "docnet_course_url"
        ))),
      by = c("centre_programme_course_code" = "guido_centre_programme_course_code")
    ) |>
    dplyr::mutate(
      # canonical join key (DOCnet triple+modality, else GUIdO triple)
      course_code = dplyr::if_else(
        source_system == "DOCnet",
        docnet_centre_programme_course_code,
        centre_programme_course_code
      ),
      # canonical url for traceability (DOCnet rows use mapped docnet url)
      course_url = dplyr::if_else(
        source_system == "DOCnet",
        docnet_course_url,
        course_url
      )
    ) |>
    dplyr::select(
      source_system, academic_year,
      centre_name, programme_name,
      
      # codes you want to keep
      centre_programme_course_code,  # always GUIdO triple
      course_code,                   # canonical (GUIdO triple or DOCnet triple+modality)
      
      # only url you want to keep
      course_url,
      
      # course metadata
      course_name, course_period, course_type, course_credits
    ) |>
    dplyr::as_tibble()
  
  # 2) Stack info tables (rowbind) into a single table keyed by course_code
  guido_info2 <- guido_course_info |>
    dplyr::transmute(
      course_code = guido_centre_programme_course_code,
      dplyr::across(dplyr::starts_with("course_"))
    )
  
  docnet_info2 <- docnet_course_info |>
    dplyr::transmute(
      course_code = docnet_centre_programme_course_code,
      dplyr::across(dplyr::starts_with("course_"))
    )
  
  info_stacked <- dplyr::bind_rows(guido_info2, docnet_info2)
  
  # 3) Final join
  out |>
    dplyr::left_join(info_stacked, by = "course_code") |>
    dplyr::as_tibble()
  
  out |>
    dplyr::left_join(info_stacked, by = "course_code") |>
    dplyr::arrange(centre_name, programme_name, course_name) |>
    dplyr::as_tibble()
  
}
