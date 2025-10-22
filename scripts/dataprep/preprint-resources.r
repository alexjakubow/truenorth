################################################################################
# Preprint Resources
#
# This script determines when open science resources are added to preprints by looking at timestamps and submission parameters in the logs for actions of interest.
#
# Inputs:
# - data/preprint.parquet (created by scripts/dataprep/preprint.r)
# - data/preprint_log.parquet (created by scripts/dataprep/preprint-log.r)
#
# Output file(s):
# - data/preprint_data.parquet
# - data/preprint_preregistration.parquet
#
# Notes:
# - If a preprint has no relevant actions in the logs, we assume that activities of interest occurred at the time of creation.
# - If the status field in the resulting datasets are NA, it likely means that the assertion actions are not enabled for associated the preprint provider.
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
  R / connect[open_parquet],
  R / helpers[tidyup]
)

# Parameters
ACTIONS <- c(
  "has_prereg_links_updated",
  "has_data_links_updated"
)

# I/O
PQROOT <- "data/preprint"
PATH_PPT <- glue("{PQROOT}.parquet")
PATH_PPT_LOG <- glue("{PQROOT}_log.parquet")
PATH_PPT_DAT <- glue("{PQROOT}_data.parquet")
PATH_PPT_REG <- glue("{PQROOT}_preregistration.parquet")


# CORE TABLES ---------------------------------------------------------------
pp_tbl <- open_dataset(PATH_PPT) |>
  select(preprint_id, created, has_prereg_links, has_data_links)
pplog_tbl <- open_dataset(PATH_PPT_LOG) |>
  select(preprint_id, action, created, params)


# DATA ----------------------------------------------------------------------
ACTION <- "has_data_links_updated"
data_logged <- pplog_tbl |>
  filter(action == ACTION) |>
  to_duckdb() |>
  mutate(
    params = ifelse(grepl("available", params), "data", "no-data")
  ) |>
  rename(
    data_status = params,
    data_status_date = created
  ) |>
  select(preprint_id, data_status, data_status_date)

data_always <- pp_tbl |>
  to_duckdb() |>
  select(preprint_id, has_data_links, created) |>
  anti_join(distinct(data_logged, preprint_id), by = "preprint_id") |>
  mutate(
    data_status = if_else(has_data_links == "available", "data", "no data"),
    data_status_date = created
  ) |>
  select(preprint_id, data_status, data_status_date)

tbl_data <- bind_rows(
  collect(data_always),
  collect(data_logged)
)

write_parquet(tbl_data, PATH_PPT_DAT)
rm(tbl_data)


# PREREGISTRATION -----------------------------------------------------------
ACTION <- "has_prereg_links_updated"
prereg_logged <- pplog_tbl |>
  filter(action == ACTION) |>
  to_duckdb() |>
  mutate(
    params = ifelse(grepl("available", params), "prereg", "no-prereg")
  ) |>
  rename(
    prereg_status = params,
    prereg_status_date = created
  ) |>
  select(preprint_id, prereg_status, prereg_status_date)

prereg_always <- pp_tbl |>
  to_duckdb() |>
  select(preprint_id, has_prereg_links, created) |>
  anti_join(distinct(prereg_logged, preprint_id), by = "preprint_id") |>
  mutate(
    prereg_status = if_else(
      has_prereg_links == "available",
      "prereg",
      "no prereg"
    ),
    prereg_status_date = created
  ) |>
  select(preprint_id, prereg_status, prereg_status_date)

tbl_prereg <- bind_rows(
  collect(prereg_always),
  collect(prereg_logged)
)

write_parquet(tbl_prereg, PATH_PPT_REG)
rm(tbl_prereg)
