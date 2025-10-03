# Packages
library(arrow)
library(dbplyr)
library(dplyr)
library(lubridate)
library(tidyr)
library(purrr)

# Modules
box::use(
  R / parameters[DATES],
  R / helpers[tidyup],
  R / database[reg_badge_status]
)

# I/O
PATHIN <- "data/registration_badges.parquet"
PATHOUT <- "data/registration_badges_ts.parquet"


# FUNCTIONS -----------------------------------------------------------------
badges_count_monthly <- function(dates) {
  purrr::map(
    dates,
    ~ reg_badge_status(reg_badges, .x, node_id, artifact_type) |>
      collect(),
    .progress = TRUE
  ) |>
    tidyup(names_src = dates, names_to = "date") |>
    arrange(date, node_id)
}


# MAIN ----------------------------------------------------------------------
reg_badges <- open_dataset(PATH_REG_BDG) |>
  to_duckdb()
reg_badges_monthly <- badges_count_monthly(DATES)
write_parquet(reg_badges_monthly, PATHOUT)
