################################################################################
# Registration Badges
#
# This script determines when open science resources (i.e., badges) are added to registrations by looking at timestamps and submission parameters in the logs for all actions of type "resource_identifier_added".
#
# Inputs:
# - data/registration.parquet (created by scripts/dataprep/registration.r)
# - osf_nodelog (database)
# - osf_outcomeartifact (database)
#
# Output file:
# - data/registration_badges.parquet
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

# I/O
PATHOUT <- "data/registration_badges.parquet"


# HELPER FUNCTIONS ----------------------------------------------------------
assign_badge <- function(x) {
  dplyr::case_when(
    x == 1 ~ "data",
    x == 11 ~ "code",
    x == 21 ~ "materials",
    x == 31 ~ "papers",
    x == 41 ~ "supplements"
  )
}


# CORE TABLES ---------------------------------------------------------------
reg_tbl <- open_parquet("data", "registration")
nodelog_tbl <- open_parquet(tbl = "osf_nodelog")
outcomeartifact_tbl <- open_parquet(tbl = "osf_outcomeartifact")


# PREP ----------------------------------------------------------------------
# Reduce tables
reg_tbl <- reg_tbl |>
  select(node_id)
resadded_tbl <- nodelog_tbl |>
  filter(action == "resource_identifier_added") |>
  select(node_id, params)
outcomeartifact_tbl <- outcomeartifact_tbl |>
  select(artifact_id = `_id`, artifact_type, created, deleted, finalized)

# Link registrations to artifact_types via parsed `params` field in log
reg_adds <- reg_tbl |>
  inner_join(resadded_tbl, by = "node_id") |>
  collect() |>
  mutate(
    params = str_extract(params, pattern = ("[0-9][0-9a-z]{5,}"))
  ) |>
  rename(artifact_id = params) |>
  left_join(collect(outcomeartifact_tbl), by = "artifact_id") |>
  select(-artifact_id) |>
  mutate(
    artifact_type = assign_badge(artifact_type)
  )


# EXPORT --------------------------------------------------------------------
reg_adds |>
  write_parquet(PATHOUT)
