# Packages
library(arrow)
library(dbplyr)
library(dplyr)
library(lubridate)
library(tidyr)
library(purrr)

# Modules
box::use(
  R / parameters[DATES, OUTPUTS, OUTCOMES],
  R / helpers[tidyup],
  R / database[reg_badge_status]
)


# I/O
PATH_REG <- "data/registration.parquet"
PATH_REG_BDG <- "data/registration_badges.parquet"
PATHOUT <- "data/registration_badges_ttlos.parquet"


# GET OSR REGISTRATIONS -------------------------------------------------------
# Load
reg_tbl <- open_dataset(PATH_REG) |>
  to_duckdb()

# Subset on OSR registrations (today)
reg_osr <- reg_tbl |>
  filter(
    is.na(deleted),
    !is.na(registered_date),
    is_spam == 0,
    is_public == TRUE
  ) |>
  select(node_id, date_registered = registered_date, registry, template)


# DETERMINE LIFECYCLE MOMENTS ----------------------------------------------------
# Load
badges_tbl <- open_dataset(PATH_REG_BDG) |>
  to_duckdb()

# Compute dates of lifecycle moments
reg_moments <- badges_tbl |>
  filter(finalized == TRUE & is.na(deleted)) |>
  summarize(
    .by = node_id,
    # Counts
    n_code = sum(artifact_type == "code", na.rm = TRUE),
    n_data = sum(artifact_type == "data", na.rm = TRUE),
    n_materials = sum(artifact_type == "materials", na.rm = TRUE),
    n_supplements = sum(artifact_type == "supplements", na.rm = TRUE),
    n_papers = sum(artifact_type == "papers", na.rm = TRUE),
    # First date of each badge type (and among all outcomes)
    date1_code = min(created[artifact_type == "code"], na.rm = TRUE),
    date1_data = min(created[artifact_type == "data"], na.rm = TRUE),
    date1_materials = min(created[artifact_type == "materials"], na.rm = TRUE),
    date1_supplements = min(
      created[artifact_type == "supplements"],
      na.rm = TRUE
    ),
    date1_papers = min(created[artifact_type == "papers"], na.rm = TRUE),
    date1_outcome = min(
      created[artifact_type %in% OUTCOMES],
      na.rm = TRUE
    ),
    date1_output = min(created[artifact_type %in% OUTPUTS], na.rm = TRUE),
    # Last date of each badge type (and among all outcomes)
    daten_code = max(created[artifact_type == "code"], na.rm = TRUE),
    daten_data = max(created[artifact_type == "data"], na.rm = TRUE),
    daten_materials = max(created[artifact_type == "materials"], na.rm = TRUE),
    daten_supplements = max(
      created[artifact_type == "supplements"],
      na.rm = TRUE
    ),
    daten_papers = max(created[artifact_type == "papers"], na.rm = TRUE),
    daten_outcome = max(
      created[artifact_type %in% OUTCOMES],
      na.rm = TRUE
    ),
    # Which outcomes and outputs came first
    first_output = case_when(
      min(created[artifact_type %in% OUTPUTS]) ==
        min(created[artifact_type == "data"]) ~
        "data",
      min(created[artifact_type %in% OUTPUTS]) ==
        min(created[artifact_type == "materials"]) ~
        "materials",
      min(created[artifact_type %in% OUTPUTS]) ==
        min(created[artifact_type == "code"]) ~
        "code",
      min(created[artifact_type %in% OUTPUTS]) ==
        min(created[artifact_type == "supplements"]) ~
        "supplements",
      .default = NA
    ),
    first_outcome = "papers" # IDEA: diversify based on outgoing link type
  ) |>
  mutate(
    first_link = case_when(
      date1_outcome < date1_output ~ "outcome",
      date1_outcome > date1_output ~ "output",
      date1_outcome == date1_output ~ "both",
      .default = NA
    ),
    date_lifecycle = case_when(
      date1_outcome < date1_output ~ date1_output,
      date1_outcome >= date1_output ~ date1_outcome,
      .default = NA
    )
  ) |>
  collect()

# CREATE TIME-TO-LOS TABLE -------------------------------------------------------
write_parquet(reg_moments, PATHOUT)
