################################################################################
# Create Registration Log Table
#
# This script creates a log table of relevant open science actions for all registrations in the database.
#
# Inputs:
#   - Actions of interest (see `OPEN_ACTIONS` and `CLOSED_ACTIONS`)
#   - data/registration.parquet (created by scripts/dataprep/registrations.r)
#   - osf_nodelog (from database)
#
# Output file:
#   - data/registration_log.parquet
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
  R / criteria[SPAM_DEF, CRITERIA_OSR],
)

# Parameters
OPEN_ACTIONS <- c(
  "made_public",
  "embargo_cancelled",
  "embargo_terminated",
  "embargo_completed",
  "retraction_cancelled",
  "registration_approved"
)
CLOSED_ACTIONS <- c(
  "made_private",
  "embargo_approved",
  "retraction_approved",
  "confirm_spam"
)

# I/O
PQROOT <- "data/registration"
PATH_REG <- glue("{PQROOT}.parquet")
PATH_REG_LOG <- glue("{PQROOT}_log.parquet")

# Open tables
nodelog_tbl <- open_parquet(tbl = "osf_nodelog", duck = FALSE)
reg_tbl <- open_dataset(PATH_REG)

# Create registration log table, filtering on actions of interest
reg_tbl |>
  select(node_id) |>
  left_join(
    filter(nodelog_tbl, action %in% c(OPEN_ACTIONS, CLOSED_ACTIONS)),
    by = "node_id"
  ) |>
  write_parquet(PATH_REG_LOG)
