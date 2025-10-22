################################################################################
# Create Preprint Log Table
#
# This script creates a log table of relevant open science actions for all preprints in the database.
#
# Inputs:
#   - Actions of interest (see `OPEN_ACTIONS` and `CLOSED_ACTIONS`)
#   - data/preprint.parquet (created by scripts/dataprep/preprints.r)
#   - osf_preprintlog (from database)
#
# Output file:
#   - data/preprint_log.parquet
################################################################################

# Packages
library(arrow)
library(dbplyr)
library(dplyr)
library(glue)
library(lubridate)
library(stringr)
library(tidyr)
library(purrr)

# Modules
box::use(
  R / connect[open_parquet]
)

# Parameters
OPEN_ACTIONS <- c(
  "made_public",
  "has_prereg_links_updated",
  "has_data_links_updated"
)
CLOSED_ACTIONS <- c(
  "made_private",
  "confirm_spam"
)

# I/O
PQROOT <- "data/preprint"
PATH_PPT <- glue("{PQROOT}.parquet")
PATH_PPT_LOG <- glue("{PQROOT}_log.parquet")

# Open tables
pptlog_tbl <- open_parquet(tbl = "osf_preprintlog", duck = FALSE)
ppt_tbl <- open_dataset(PATH_PPT)

# Create preprint log table, filtering on actions of interest
ppt_tbl |>
  select(preprint_id) |>
  left_join(
    filter(pptlog_tbl, action %in% c(OPEN_ACTIONS, CLOSED_ACTIONS)),
    by = "preprint_id"
  ) |>
  write_parquet(PATH_PPT_LOG)
