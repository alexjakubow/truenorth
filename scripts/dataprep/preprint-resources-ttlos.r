# Packages
library(arrow)
library(dbplyr)
library(dplyr)
library(lubridate)
library(tidyr)
library(purrr)

# Modules
box::use(
  R / parameters[DATES, OUTPUTS, PLANS],
  R / helpers[tidyup],
  R / database[ppt_resource_status]
)


# I/O
PATH_PPT <- "data/preprint.parquet"
PATH_PPT_RES <- "data/preprint_resources.parquet"
PATHOUT <- "data/preprint_resources_ttlos.parquet"


# GET OSP PREPRINTS -------------------------------------------------------
# Load
ppt_tbl <- open_dataset(PATH_PPT) |>
  to_duckdb()

# Subset on OSP preprints (today)
ppt_osp <- ppt_tbl |>
  filter(
    is.na(deleted),
    !is.na(date_withdrawn),
    machine_state == "accepted",
    is_spam == 0,
    is_public == TRUE
  ) |>
  select(preprint_id, provider)


# DETERMINE LIFECYCLE MOMENTS ----------------------------------------------------
# Load
resources_tbl <- open_dataset(PATH_PPT_RES) |>
  to_duckdb()

# Compute dates of lifecycle moments
ppt_moments <- resources_tbl |>
  summarize(
    .by = preprint_id,
    # Counts
    n_data = sum(resource_type == "data", na.rm = TRUE),
    n_prereg = sum(resource_type == "prereg", na.rm = TRUE),
    # First date of each resource type (and among all outcomes)
    date1_data = min(created[resource_type == "data"], na.rm = TRUE),
    date1_prereg = min(created[resource_type == "prereg"], na.rm = TRUE),
    date1_plan = min(created[resource_type %in% PLANS], na.rm = TRUE),
    date1_output = min(created[resource_type %in% OUTPUTS], na.rm = TRUE),
    # Last date of each resource type (and among all outcomes)
    daten_data = max(created[resource_type == "data"], na.rm = TRUE),
    daten_prereg = max(created[resource_type == "prereg"], na.rm = TRUE),
    daten_plan = max(created[resource_type %in% PLANS], na.rm = TRUE),
    daten_output = max(created[resource_type %in% OUTPUTS], na.rm = TRUE),
  ) |>
  mutate(
    first_link = case_when(
      date1_output < date1_plan ~ "output",
      date1_output > date1_plan ~ "plan",
      date1_output == date1_plan ~ "both",
      .default = NA
    ),
    date_lifecycle = case_when(
      date1_plan < date1_output ~ date1_output,
      date1_plan >= date1_output ~ date1_plan,
      .default = NA
    )
  ) |>
  collect()

# CREATE TIME-TO-LOS TABLE -------------------------------------------------------
write_parquet(ppt_moments, PATHOUT)
