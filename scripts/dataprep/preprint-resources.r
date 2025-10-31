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

# I/O
PQROOT <- "data/preprint"
PATHOUT <- glue("{PQROOT}_resources.parquet")


# CORE TABLES ---------------------------------------------------------------
pplog_tbl <- open_parquet(tbl = "osf_preprintlog")


# PREP ----------------------------------------------------------------------
# Pre-registration linked dates
plan_tbl <- pplog_tbl |>
  filter(action == "has_prereg_links_updated") |>
  select(preprint_id, created, params) |>
  collect() |>
  mutate(
    params = str_extract(params, pattern = ("no|not_applicable|available")),
    resource_type = "prereg"
  )

# Data linked dates
data_tbl <- pplog_tbl |>
  filter(action == "has_data_links_updated") |>
  select(preprint_id, created, params) |>
  collect() |>
  mutate(
    params = str_extract(params, pattern = ("no|not_applicable|available")),
    resource_type = "data"
  )

# Combine and only keep if available
resource_tbl <- bind_rows(plan_tbl, data_tbl) |>
  filter(params == "available") |>
  # mutate(
  #   resource_added = case_when(
  #     params == "no" ~ 0,
  #     params == "not_applicable" ~ 0,
  #     params == "available" ~ 1
  #   )
  # ) |>
  # rename(
  #   resource_detail = params
  # ) |>
  select(preprint_id, resource_type, created, resource_type)


# EXPORT --------------------------------------------------------------------
resource_tbl |>
  write_parquet(PATHOUT)
