################################################################################
# Create Registration Table
#
# This script creates the core registration table used in many downstream operations.
#
# Input tables:
#   - osf_abstractnode (database)
#   - osf_abstractprovider (database)
#   - osf_guid (database)
#   - osf_nodelog (database)
#   - data/registration_template.parquet (scripts/dataprep/registration-template.r)
#
# Output file:
#   - /data/registration.parquet
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
  R / criteria[SPAM_DEF],
)

# I/O
PQROOT <- "data/registration"
PATH_REG <- glue("{PQROOT}.parquet")


# CORE TABLES ---------------------------------------------------------------
# Open tables
node_tbl <- open_parquet(tbl = "osf_abstractnode", duck = FALSE)
provider_tbl <- open_parquet(tbl = "osf_abstractprovider", duck = FALSE)
guid_tbl <- open_parquet(tbl = "osf_guid", duck = FALSE)
template_tbl <- open_parquet(
  dir = "data",
  tbl = "registration_template",
  duck = FALSE
)

# Munge input tables
registrations <- node_tbl |>
  filter(type == "osf.registration") |>
  select(
    node_id = id,
    is_public,
    is_spam = spam_status,
    created,
    deleted,
    registered_date,
    embargo_id,
    retraction_id,
    provider_id,
    moderation_state
  ) |>
  mutate(
    is_spam = if_else(!!SPAM_DEF, 1, 0, missing = 0)
  )
node_guids <- guid_tbl |>
  filter(content_type_id == 30) |>
  select(
    node_id = object_id,
    guid = `_id`
  )
providers <- provider_tbl |>
  select(provider_id = id, registry = name)
templates <- template_tbl |>
  select(node_id, template)


# EXPORT --------------------------------------------------------------------
# Join and export
registrations |>
  left_join(node_guids, by = "node_id") |>
  left_join(providers, by = "provider_id") |>
  left_join(templates, by = "node_id") |>
  write_parquet(PATH_REG)
