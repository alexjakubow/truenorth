################################################################################
# Preprint Events
#
# This script uses logged actions on preprints to determine when key lifecycle open science activities occur during the lifecycle of a preprint.  At present, the following activities are tracked:
# - Visibility
# - Spam
#
# Inputs:
# - data/preprint.parquet (created by scripts/dataprep/preprint.r)
# - data/preprint_log.parquet (created by scripts/dataprep/preprint-log.r)
#
# Output files:
# - data/preprint_visibility.parquet
# - data/preprint_spam.parquet
#
# Notes:
# - If a preprint has no relevant actions in the logs, we assume that activities of interest occurred at the time of creation.
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

# I/O
PQROOT <- "data/preprint"
PATH_PPT <- glue("{PQROOT}.parquet")
PATH_PPT_LOG <- glue("{PQROOT}_log.parquet")
PATH_PPT_VIS <- glue("{PQROOT}_visibility.parquet")
PATH_PPT_SPM <- glue("{PQROOT}_spam.parquet")
PATH_PPT_RES <- glue("{PQROOT}_resources.parquet")


# VISIBILITY ----------------------------------------------------------------
ACTIONS <- c("made_public", "made_private")
visibility_logged <- open_dataset(PATH_PPT_LOG) |>
  select(preprint_id, action, created) |>
  filter(action %in% ACTIONS) |>
  mutate(action = if_else(action == "made_public", "public", "private")) |>
  rename(
    visibility_status = action,
    visibility_status_date = created
  )

visibility_always <- open_dataset(PATH_PPT) |>
  select(preprint_id, is_public, created) |>
  anti_join(distinct(visibility_logged, preprint_id), by = "preprint_id") |>
  mutate(visibility_status = if_else(is_public, "public", "private")) |>
  rename(visibility_status_date = created) |>
  select(preprint_id, visibility_status, visibility_status_date)

tbl_visibility <- bind_rows(
  collect(visibility_always),
  collect(visibility_logged)
)

write_parquet(tbl_visibility, PATH_PPT_VIS)
rm(tbl_visibility)


# SPAM -------------------------------------------------------------------------
ACTIONS <- "confirm_spam"
spam_logged <- open_dataset(PATH_PPT_LOG) |>
  select(preprint_id, action, created) |>
  filter(action %in% ACTIONS) |>
  to_duckdb() |>
  mutate(
    action = "spam"
  ) |>
  select(preprint_id, action, created) |>
  rename(
    spam_status = action,
    spam_status_date = created
  )
spam_always <- open_dataset(PATH_PPT) |>
  to_duckdb() |>
  select(preprint_id, is_spam, created) |>
  anti_join(distinct(spam_logged, preprint_id), by = "preprint_id") |>
  mutate(
    spam_status = if_else(is_spam == 1, "spam", "non-spam"),
    spam_status_date = created
  ) |>
  select(preprint_id, spam_status, spam_status_date)

tbl_spam <- bind_rows(
  collect(spam_always),
  collect(spam_logged)
)

write_parquet(tbl_spam, PATH_PPT_SPM)
rm(tbl_spam)
