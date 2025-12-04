# -------------------------------------------------------------------
# Load directory resolution and data download
# -------------------------------------------------------------------

resolve_load_dir <- function(load_cfg) {
  root   <- load_cfg$root %||% "data"
  dir    <- load_cfg$dir
  subdir <- load_cfg$subdir
  
  if (is.null(dir) || !nzchar(dir)) stop("dir is not set in config/load.yml.")
  if (is.null(subdir) || !nzchar(subdir)) return(file.path(root, dir))
  
  file.path(root, dir, subdir)
}

ensure_load_data <- function(load_cfg) {
  root           <- load_cfg$root %||% "data"
  dir            <- load_cfg$dir
  subdir         <- load_cfg$subdir
  url            <- load_cfg$url
  archive_path   <- load_cfg$archive_path %||% file.path(root, paste0(dir, ".zip"))
  force_download <- isTRUE(load_cfg$force_download %||% FALSE)
  is_zip         <- isTRUE(load_cfg$zip %||% TRUE)
  
  if (is.null(dir) || !nzchar(dir)) stop("dir is not set in config/load.yml.")
  
  base_dir <- file.path(root, dir)
  load_dir <- if (is.null(subdir) || !nzchar(subdir)) base_dir else file.path(base_dir, subdir)
  
  if (dir.exists(load_dir)) {
    message("Using existing load directory: ", load_dir)
    return(load_dir)
  }
  
  if (!dir.exists(base_dir)) {
    if (is.null(url) || !nzchar(url)) {
      stop(
        "Load directory does not exist: ", load_dir,
        "\nRepository root does not exist either: ", base_dir,
        "\nNo 'url' provided in pipeline config under load$url, ",
        "so data cannot be downloaded automatically."
      )
    }
    
    if (!dir.exists(root)) dir.create(root, recursive = TRUE, showWarnings = FALSE)
    dir.create(dirname(archive_path), recursive = TRUE, showWarnings = FALSE)
    
    if (force_download || !file.exists(archive_path)) {
      message("Repository root not found (", base_dir, ").")
      message("Will download archive from: ", url)
      message("Saving archive to: ", archive_path)
      utils::download.file(url, archive_path, mode = "wb")
    } else {
      message("Repository root not found (", base_dir, ").")
      message("Using existing archive file: ", archive_path)
    }
    
    if (is_zip) {
      utils::unzip(archive_path, exdir = root)
      message("Unzipped archive into: ", root)
    } else {
      dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
      file.copy(archive_path, file.path(base_dir, basename(archive_path)), overwrite = TRUE)
      message("Copied downloaded file into: ", base_dir)
    }
  }
  
  if (!dir.exists(base_dir)) {
    stop(
      "Repository root does not exist after download/unzip: ", base_dir,
      "\nCheck the archive structure or adjust dir in config/load.yml."
    )
  }
  
  if (!dir.exists(load_dir)) {
    stop(
      "Load directory not found after download/unzip: ", load_dir,
      "\nCheck that the archive contains this scraping subfolder or ",
      "adjust subdir in config/load.yml."
    )
  }
  
  message("Using load directory: ", load_dir)
  load_dir
}
