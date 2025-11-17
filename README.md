# URV SDGs tracker

Tracking how the URV degree programmes contribute to the UN Sustainable Development Goals (SDGs) through their official course guides.

The project reads and processes the URV teaching guides (guies docents) and builds a structured, analysable dataset that can be used to:

- quantify how often and where SDGs appear in the curriculum,
- explore differences between faculties, degrees and course types,
- support institutional monitoring and quality assurance around SDG alignment.

At this stage the focus is on **URV** programmes only.

---

## Context: Docnet → GUIA docent (GUIdO) transition

The URV is currently in the middle of a transition between two teaching guide systems:

- **DOCNET** – legacy system used for many degrees.
- **GUIA docent (GUIdO)** – the new guide platform, gradually replacing DOCNET.

As a consequence:

- some degrees still have their guides in DOCNET,
- others are already in GUIdO,
- and a few may have mixed or changing structures across academic years.

This project explicitly handles **both** sources:

- course metadata and textual fields are scraped from **DOCNET** *and* **GUIdO**, 
- then harmonised into a single per-course dataset.

Because the platforms are evolving, some steps of the process are currently **semi-automatic** (see below), but they are still orchestrated through the same pipeline so that runs are reproducible once the inputs are fixed.

---

## Project goals

1. **Map the curriculum to the SDGs**  
   Build a course-level corpus where each row corresponds to a URV course and includes:
   - programme and faculty information,
   - course metadata (code, period, credits, type, year…),
   - concatenated textual fields: description, contents, competences, learning outcomes, bibliography,
   - staff information (coordinators and lecturers).

2. **Detect SDG signals in course texts**  
   Use `text2sdg` and related text-mining tools to:
   - detect potential mentions and alignments with SDGs,
   - explore differences across faculties, degrees and course types.

3. **Provide a reproducible analysis environment**  
   Using:
   - a **Docker** image with all R dependencies,
   - a pipeline built with **{targets}**, 
   - configuration files instead of hard-coded paths.

4. **Publish a static dashboard**  
   A static site under `docs/` (GitHub Pages-compatible) will expose summary indicators and exploratory dashboards for URV stakeholders.

---

## High-level pipeline

The analysis is organised as a set of pipeline phases, implemented with [{targets}](https://books.ropensci.org/targets/). Each phase adds structure but avoids doing too much at once.

### 00 – scraping (configuration only for now)

Current situation:

- Scraping is performed **externally** using a browser plugin such as **Web Scraper** (Firefox/Chrome).
- The plugin exports a set of CSV files per batch (degrees, course details, descriptions, contents, competences, bibliography, etc.).
- These CSVs are stored in a dedicated folder:

  - archived runs (versioned):  
    `data/scrapings_archive/guides_scraping_YYYYMMDD/`
  - experimental / temporary runs:  
    `sandbox/data/guides_scraping_YYYYMMDD/`

Configuration is handled via `config/scraping.yml`:

- `mode`: `"existing"` (use an existing scraping) or `"new"` (future internal scraping).
- `input_location`: `"archive"` or `"sandbox"`.
- `run_id`: the batch identifier, typically `YYYYMMDD`.

The **00_scraping** phase does **not** scrape the web yet. It:

- reads the config file,
- decides which folder to use,
- checks that it exists,
- and records this as the `scraping_dir` target.

There is a deliberate placeholder message documenting that scraping is still external and suggesting the use of a browser plugin until the R-based scraper is implemented.

### 01 – import (raw → structured index)

The **01_import** phase takes the CSVs from a single scraping batch and:

1. **Reads all raw tables**:

   - `degree_programs_list.csv`
   - `course_details_list_guido.csv`
   - `course_details_list_docnet.csv`
   - `docnet-iframe.csv`
   - `guido-course-coordinators.csv`, `docnet-course-coordinators.csv`
   - `guido-course-professors.csv`, `docnet-course-professors.csv`
   - `guido-course-description.csv`, `docnet-course-description.csv`
   - `guido-course-contents.csv`, `docnet-course-contents.csv`
   - `guido-course-learning-results.csv`, `docnet-course-learning-results.csv`
   - `docnet-course-competences.csv`
   - `guido-course-bibliography.csv`, `docnet-course-bibliography.csv`

2. **Harmonises DOCNET and GUIdO structures**:

   - aligns column names (e.g. `course_url`, `course_code`, `course_name`),
   - merges the two systems into unified tables (e.g. `course_details`, `course_description`),
   - uses the `docnet-iframe` mapping to resolve GUIdO iframes in DOCNET courses.

3. **Builds a per-course index**: `guides_index`

   A single row per `course_url`, including:

   - degree and faculty information:
     - `faculty_school_name` (derived from `faculty_school_url` using a mapping function)
     - `faculty_school_url`
     - `degree_name`, `degree_url`, `degree_year`
   - basic course metadata:
     - `course_code`, `course_name`, `course_delivery_mode`,
     - `course_period`, `course_type`, `credits`, `year`
   - staff:
     - `coordinators`, `professors` (aggregated into `;`-separated strings)
   - textual fields (no cleaning, no translation yet):
     - `description`, `contents`, `competences_learning_results`, `references`  
       each aggregated per course as a `;`-separated concatenation
   - an internal `document_number` (row index) mainly for analysis and identification.

The goal of this phase is a **clean structural index** of all courses, without touching the actual language or text noise.

### 02 – clean (text normalisation and basic tidying)

The **02_clean** phase takes `guides_index` and produces `guides_clean` by:

- normalising credits and numeric fields (`credits`, `year`),
- standardising course and degree names where needed,
- applying light text normalisation (e.g. whitespace, problematic apostrophes),
- keeping both original and cleaned versions of key fields where it makes sense.

The idea is to make the dataset **ready for translation and SDG detection**, without losing information that could be useful diagnostically.

### Next phases (planned)

These phases are planned but not fully implemented yet:

- **03_translate** – translate relevant textual fields into English (e.g. with LibreTranslate or Apertium), keeping both original and translated text.
- **04_detect_sdg** – apply `text2sdg`, keyword dictionaries and other methods to assign potential SDG labels to each course.
- **05_validation** – manual or semi-automatic validation of SDG assignments.
- **06_export** – produce tidy tables for dashboards and reporting.
- **07_analysis** – generate figures, summary tables and the static website under `docs/`.

---

## Reproducibility and tools

The project is designed to be reproducible and portable across machines and operating systems.

### Docker + RStudio

A `Dockerfile` and `docker-compose.yml` are provided to run the whole project inside a containerised environment with:

- R and RStudio Server,
- the **tidyverse**, 
- text and SDG-related packages,
- pipeline tooling (`targets`),
- I/O and Excel support (`readr`, `openxlsx`, etc.).

Typical usage:

```bash
docker-compose up
```

Then open RStudio in your browser, set the working directory to the project root (if needed), and run the pipeline from there.

### R packages and pipeline engine

Key R packages include:

- **Workflow & reproducibility**
  - [`targets`](https://docs.ropensci.org/targets/) – pipeline engine; tracks dependencies and avoids unnecessary recomputation.
- **Data wrangling and I/O**
  - `tidyverse` (including `dplyr`, `readr`, `stringr`, `purrr`, `tibble`, `tidyr`)
  - `openxlsx` – Excel I/O when needed.
  - `jsonlite` – JSON I/O.
- **Text & SDG analysis**
  - `text2sdg` – SDG-related text detection.
  - `corpustools`, `tidytext`, `quanteda`, `stopwords` – additional text-mining tools.

All these libraries are installed in the Docker image so that the environment is consistent across runs.

You can additionally manage local R dependencies with [`renv`](https://rstudio.github.io/renv/) if you prefer to run outside Docker, but Docker is the recommended route for institutional reproducibility.

---

## Semi-automatic steps and limitations

Because of the current state of the URV platforms, some parts of the workflow are **semi-automatic**:

- **Scraping**:
  - data are extracted using a browser plugin (e.g. Web Scraper),
  - the plugin configuration reflects DOCNET and GUIdO structures, which may evolve,
  - the exported CSVs are then placed into `data/scrapings_archive/guides_scraping_YYYYMMDD/`.

- **Cleaning rules**:
  - some text cleaning rules (e.g. for bullet characters, prefixes like "NA", CRAI access notes in the bibliography) have been tuned manually based on inspection,
  - these rules are codified in the pipeline, but may need adjustment when the underlying platforms change.

The pipeline tries to **isolate** these fragile steps:

- Once a given scraping batch is archived under a specific `run_id`, running the pipeline with that `run_id` is reproducible.
- New scrapes go into a **new** `guides_scraping_YYYYMMDD` folder and a new `run_id`, so that old results are preserved and comparable.

Future work includes:

- moving scraping fully inside R,
- improving automatic detection of SDG-related content,
- and adding validation tools for academic staff.

---

## Getting started

1. **Clone the repository**

   ```bash
   git clone https://github.com/<org>/urv-sdgs-tracker.git
   cd urv-sdgs-tracker
   ```

2. **Configure scraping input**

   Edit `config/scraping.yml` to point to an existing scraping batch, for example:

   ```yaml
   mode: "existing"
   input_location: "archive"
   run_id: "20250731"
   ```

   and ensure that the directory:

   ```text
   data/scrapings_archive/guides_scraping_20250731/
   ```

   contains the CSV files exported from your scraping tool.

3. **Start the Docker environment**

   ```bash
   docker-compose up
   ```

   Open RStudio in your browser, select the project root, and open `run_pipeline.R`.

4. **Run the pipeline**

   In the R console (inside RStudio):

   ```r
   source("run_pipeline.R")

   # run the full pipeline (scraping config + import + clean)
   run_pipeline("all")

   # or run a subset, for example:
   run_pipeline("import")  # up to guides_index
   run_pipeline("clean")   # ensure guides_clean is built
   ```

5. **Inspect the data**

   ```r
   library(targets)

   tar_load(guides_index)
   tar_load(guides_clean)

   dplyr::glimpse(guides_index)
   dplyr::glimpse(guides_clean)
   ```

   These tables are the input for translation, SDG detection and dashboarding.

---

## Running tests

The project includes a small test suite based on **testthat** to check the main helpers and import logic.

1. **From the project root, open RStudio (or an R console)**

   ```r
   # setwd("~/git/urv-sdgs-tracker") # Not necessary when using the docker environment
   ```

2. **Run all tests via the convenience script**

   ```r
   source("run_tests.R")
   ```

   This script calls `testthat::test_dir("tests/testthat")` and will
   execute all `test_*.R` files under `tests/testthat/`.

3. **Run a single test file (optional)**

   If you want to focus on one area, you can run an individual file:

   ```r
   library(testthat)

   test_file("tests/testthat/test_resolve_import_dir.R")
   test_file("tests/testthat/test_build_guides_raw.R")
   ```

4. **What the tests cover (so far)**

   * `resolve_import_dir()` and `load_scraping_config()` (config handling)
   * `build_guides_raw()` and `get_guides_raw()` (CSV-based import)
   * `build_guides_index_from_raw()` (per-course index aggregation)
   * `assign_faculty_name()` (mapping centre URLs to faculty names)

   The tests are written to be fast and to use only small, local
   fixtures under `tests/testthat/testdata`, so they do not require
   the full Docker pipeline to be running.



## Roadmap

- [ ] Implement fully automated R-based scraping for DOCNET and GUIdO.
- [ ] Add translation phase (03_translate) with configurable services and caching.
- [ ] Integrate `text2sdg` and complementary methods for SDG detection.
- [ ] Build validation tools for educational staff.
- [ ] Publish and maintain a static dashboard in `docs/`.
- [ ] Document additional academic years and compare evolution over time.

---

## Acknowledgements

This project is developed within the Universitat Rovira i Virgili (URV) as part of the institutional effort to align teaching with the UN Sustainable Development Goals and to monitor the presence of SDGs in the curriculum in a transparent and reproducible way.

