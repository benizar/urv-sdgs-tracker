# File: src/pipelines/01_load/01_load_targets.R

# Targets for the load phase.

library(targets)

targets_load <- list(
  
  # load_config is provided by 00_config (config/load.yml)
  
  # Resolve/load directory (and download/unzip if needed).
  tar_target(
    load_dir,
    ensure_load_data(load_config)
  ),
  
  # Individual raw-but-structured tables.
  tar_target(
    centres_list,
    get_centres_list(load_dir)
  ),
  
  tar_target(
    programmes_list,
    get_programmes_list(load_dir)
  ),
  
  tar_target(
    course_details_list,
    get_course_details_list(load_dir)
  ),
  
  tar_target(
    guido_docnet_course_code_map,
    get_guido_docnet_course_code_map(load_dir)
  ),
  
  tar_target(
    docnet_course_info,
    get_docnet_course_info(load_dir)
  ),
  
  tar_target(
    guido_course_info,
    get_guido_course_info(load_dir)
  ),
  
  # Master table for this phase (one data.frame).
  # IMPORTANT: downstream phases expect a `document_number` id column.
  # For now we use the canonical `course_code` as `document_number`.
  tar_target(
    guides_loaded,
    {
      df <- build_guides_index_from_loaded(
        centres_list,
        programmes_list,
        course_details_list,
        guido_docnet_course_code_map,
        docnet_course_info,
        guido_course_info
      )
      
      dplyr::mutate(
        df,
        document_number = as.character(course_code)
      )
    }
  )
)
