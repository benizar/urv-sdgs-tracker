# Note: rocker/tidyverse already includes the core tidyverse packages:
#   ggplot2, dplyr, tidyr, readr, purrr, tibble, stringr, forcats.
FROM rocker/tidyverse:4.5.1

## ---- global options ----

# Default CRAN mirror
ENV CRAN_REPO=https://cran.rstudio.com/

## ---- environment & workflow tools ----
# Parallel processing, progress reporting, reproducible environments
RUN install2.r --error --repos "$CRAN_REPO" \
    furrr \
    progressr \
    renv \
    targets

## ---- data management & I/O ----
# Extra I/O packages not included in tidyverse
RUN install2.r --error --repos "$CRAN_REPO" \
    jsonlite \
    openxlsx \
    yaml
    # optional:
    # readxl

## ---- text analysis & SDG detection ----
# Core packages for SDG detection from text
RUN install2.r --error --repos "$CRAN_REPO" \
    text2sdg \
    text2sdgData \
    corpustools \
    tidytext \
    quanteda \
    stopwords

## ---- testing & quality assurance ----
# Unit testing
RUN install2.r --error --repos "$CRAN_REPO" \
    testthat

## ---- general analysis / modelling ----
RUN install2.r --error --repos "$CRAN_REPO" \
    ranger

## ---- visualization & reporting ----
RUN install2.r --error --repos "$CRAN_REPO" \
    cowplot \
    gtsummary \
    patchwork

