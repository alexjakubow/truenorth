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
OPEN_ACTIONS <- c("made_public")
CLOSED_ACTIONS <- c("made_private", "confirm_spam")

# I/O
PQROOT <- "data/preprint"
PATH_PPT <- glue("{PQROOT}.parquet")
PATH_PPT_LOG <- glue("{PQROOT}_log.parquet")
PATH_PPT_VIS <- glue("{PQROOT}_visibility.parquet")
PATH_PPT_SPM <- glue("{PQROOT}_spam.parquet")


# CORE TABLES ---------------------------------------------------------------
# Open tables
preprint_tbl <- open_parquet(tbl = "osf_preprint", duck = FALSE)
guid_tbl <- open_parquet(tbl = "osf_guid", duck = FALSE)
provider_tbl <- open_parquet(tbl = "osf_abstractprovider", duck = FALSE)
pplog_tbl <- open_parquet(tbl = "osf_preprintlog", duck = FALSE)

# Create preprint table
preprints <- preprint_tbl |>
  select(
    preprint_id = id,
    provider_id,
    is_public,
    is_spam = spam_status,
    created,
    deleted,
    date_withdrawn,
    date_published,
    machine_state,
    has_data_links,
    has_prereg_links
  ) |>
  mutate(
    is_spam = if_else(is_spam == 2, 1, 0, missing = 0)
  )
preprint_guids <- guid_tbl |>
  filter(content_type_id == 47) |>
  select(
    preprint_id = object_id,
    guid = `_id`
  )
providers <- provider_tbl |>
  select(provider_id = id, registry = name)
preprints |>
  left_join(preprint_guids, by = "preprint_id") |>
  left_join(providers, by = "provider_id") |>
  write_parquet(PATH_PPT)


# Create preprint log table, subsetting on actions of interest
pplog_subset_tbl <- pplog_tbl |>
  filter(action %in% c(OPEN_ACTIONS, CLOSED_ACTIONS))
open_dataset(PATH_PPT) |>
  select(preprint_id) |>
  left_join(pplog_subset_tbl, by = "preprint_id") |>
  write_parquet(PATH_PPT_LOG)


# STATUS LOG TABLES ------------------------------------------------------------
# Visibility
ACTIONS <- c("made_public", "made_private")
visibility_logged <- open_dataset(PATH_PPT_LOG) |>
  select(preprint_id, action, created) |>
  filter(action %in% ACTIONS) |>
  mutate(action = ifelse(action == "made_public", "public", "private")) |>
  rename(
    visibility_status = action,
    visibility_status_date = created
  )

visibility_always <- open_dataset(PATH_PPT) |>
  select(preprint_id, is_public, created) |>
  anti_join(distinct(visibility_logged, preprint_id), by = "preprint_id") |>
  mutate(visibility_status = ifelse(is_public, "public", "private")) |>
  rename(visibility_status_date = created) |>
  select(preprint_id, visibility_status, visibility_status_date)

tbl_visibility <- bind_rows(
  collect(visibility_always),
  collect(visibility_logged)
)

write_parquet(tbl_visibility, PATH_PPT_VIS)
rm(tbl_visibility)

# Spam
ACTIONS <- "confirm_spam"
spam_logged <- open_dataset(PATH_PPT_LOG) |>
  select(preprint_id, action, created) |>
  filter(action %in% ACTIONS) |>
  to_duckdb() |>
  mutate(action = "spam") |>
  rename(
    spam_status = action,
    spam_status_date = created
  )
spam_always <- open_dataset(PATH_PPT) |>
  to_duckdb() |>
  select(preprint_id, is_spam, created) |>
  anti_join(distinct(spam_logged, preprint_id), by = "preprint_id") |>
  mutate(spam_status = ifelse(is_spam, "spam", "non-spam")) |>
  rename(spam_status_date = created) |>
  select(preprint_id, spam_status, spam_status_date)

tbl_spam <- bind_rows(
  collect(spam_always),
  collect(spam_logged)
)

write_parquet(tbl_spam, PATH_PPT_SPM)
rm(tbl_spam)
