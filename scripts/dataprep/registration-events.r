################################################################################
# Registration Events
#
# This script uses logged actions on registrations to determine when key lifecycle open science activities occur during the lifecycle of a registration.  At present, the following activities are tracked:
# - Visibility
# - Embargo
# - Retraction
# - Spam
#
# Inputs:
# - data/registration.parquet (created by scripts/dataprep/registration.r)
# - data/registration_log.parquet (created by scripts/dataprep/registration-log.r)
#
# Output files:
# - data/registration_visibility.parquet
# - data/registration_embargo.parquet
# - data/registration_retraction.parquet
# - data/registration_spam.parquet
#
# Notes:
# - If a registration has no relevant actions in the logs, we assume that activities of interest occurred at the time of creation.
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
PQROOT <- "data/registration"
PATH_REG <- glue("{PQROOT}.parquet")
PATH_REG_LOG <- glue("{PQROOT}_log.parquet")
PATH_REG_VIS <- glue("{PQROOT}_visibility.parquet")
PATH_REG_EMB <- glue("{PQROOT}_embargo.parquet")
PATH_REG_RET <- glue("{PQROOT}_retraction.parquet")
PATH_REG_SPM <- glue("{PQROOT}_spam.parquet")


# VISIBILITY ----------------------------------------------------------------
ACTIONS <- c("made_public", "made_private")
visibility_logged <- open_dataset(PATH_REG_LOG) |>
  select(node_id, action, created) |>
  filter(action %in% ACTIONS) |>
  mutate(action = if_else(action == "made_public", "public", "private")) |>
  rename(
    visibility_status = action,
    visibility_status_date = created
  )

visibility_always <- open_dataset(PATH_REG) |>
  select(node_id, is_public, created) |>
  anti_join(distinct(visibility_logged, node_id), by = "node_id") |>
  mutate(visibility_status = if_else(is_public, "public", "private")) |>
  rename(visibility_status_date = created) |>
  select(node_id, visibility_status, visibility_status_date)

tbl_visibility <- bind_rows(
  collect(visibility_always),
  collect(visibility_logged)
)

write_parquet(tbl_visibility, PATH_REG_VIS)
rm(tbl_visibility)


# EMBARGO ----------------------------------------------------------------
ACTIONS <- c(
  "embargo_approved",
  "embargo_cancelled",
  "embargo_completed",
  "embargo_terminated"
)
embargo_logged <- open_dataset(PATH_REG_LOG) |>
  select(node_id, action, created) |>
  filter(action %in% ACTIONS) |>
  to_duckdb() |>
  mutate(
    action = case_when(
      grepl("approved", action) ~ "embargoed",
      .default = "unembargoed"
    )
  ) |>
  select(node_id, action, created) |>
  rename(
    embargo_status = action,
    embargo_status_date = created
  )

embargo_always <- open_dataset(PATH_REG) |>
  to_duckdb() |>
  select(node_id, embargo_id, created) |>
  anti_join(distinct(embargo_logged, node_id), by = "node_id") |>
  mutate(
    embargo_status = if_else(is.na(embargo_id), "unembargoed", "embargoed")
  ) |>
  rename(embargo_status_date = created) |>
  select(node_id, embargo_status, embargo_status_date)

tbl_embargo <- bind_rows(
  collect(embargo_always),
  collect(embargo_logged)
)

write_parquet(tbl_embargo, PATH_REG_EMB)
rm(tbl_embargo)


# RETRACTION ----------------------------------------------------------------
ACTIONS <- c("retraction_cancelled", "retraction_approved")
retraction_logged <- open_dataset(PATH_REG_LOG) |>
  select(node_id, action, created) |>
  filter(action %in% ACTIONS) |>
  to_duckdb() |>
  mutate(
    action = if_else(grepl("approved", action), "retracted", "non-retracted")
  ) |>
  select(node_id, action, created) |>
  rename(
    retraction_status = action,
    retraction_status_date = created
  )

retraction_always <- open_dataset(PATH_REG) |>
  to_duckdb() |>
  select(node_id, retraction_id, created) |>
  anti_join(distinct(retraction_logged, node_id), by = "node_id") |>
  mutate(
    retraction_status = if_else(
      is.na(retraction_id),
      "non-retracted",
      "retracted"
    )
  ) |>
  rename(retraction_status_date = created) |>
  select(node_id, retraction_status, retraction_status_date)

tbl_retraction <- bind_rows(
  collect(retraction_always),
  collect(retraction_logged)
)

write_parquet(tbl_retraction, PATH_REG_RET)
rm(tbl_retraction)


# SPAM -------------------------------------------------------------------------
ACTIONS <- "confirm_spam"
spam_logged <- open_dataset(PATH_REG_LOG) |>
  select(node_id, action, created) |>
  filter(action %in% ACTIONS) |>
  to_duckdb() |>
  mutate(
    action = "spam"
  ) |>
  select(node_id, action, created) |>
  rename(
    spam_status = action,
    spam_status_date = created
  )
spam_always <- open_dataset(PATH_REG) |>
  to_duckdb() |>
  select(node_id, is_spam, created) |>
  anti_join(distinct(spam_logged, node_id), by = "node_id") |>
  mutate(
    spam_status = if_else(is_spam == 1, "spam", "non-spam"),
    spam_status_date = created
  ) |>
  select(node_id, spam_status, spam_status_date)

tbl_spam <- bind_rows(
  collect(spam_always),
  collect(spam_logged)
)

write_parquet(tbl_spam, PATH_REG_SPM)
rm(tbl_spam)
