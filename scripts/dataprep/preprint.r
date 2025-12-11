################################################################################
# Create Preprint Table
#
# This script creates the core preprint table used in many downstream operations.
#
# Input tables:
#   - osf_preprint (database)
#   - osf_abstractprovider (database)
#   - osf_guid (database)
#   - osf_preprintlog (database)
#
# Output file:
#   - /data/preprint.parquet
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
  R / criteria[SPAM_DEF]
)

# I/O
PQROOT <- "data/preprint"
PATH_PPT <- glue("{PQROOT}.parquet")

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
    node_id,
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
  select(provider_id = id, provider = name)


# EXPORT -----------------------------------------------------------------------
preprints |>
  left_join(preprint_guids, by = "preprint_id") |>
  left_join(providers, by = "provider_id") |>
  write_parquet(PATH_PPT)
