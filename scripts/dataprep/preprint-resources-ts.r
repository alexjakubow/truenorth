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
  R / datamodel[open_parquet],
  R / helpers[tidyup],
  R / database[ppt_resource_status]
)

# I/O
PATHIN <- "data/preprint_resources.parquet"
PATHOUT <- "data/preprint_resources_ts.parquet"


# FUNCTIONS -----------------------------------------------------------------
resource_count_monthly <- function(tbl, dates) {
  purrr::map(
    dates,
    ~ ppt_resource_status(
      tbl = tbl,
      date = .x,
      preprint_id,
      resource_type
    ) |>
      collect(),
    .progress = TRUE
  ) |>
    tidyup(names_src = dates, names_to = "date") |>
    arrange(preprint_id, date)
}


# MAIN ----------------------------------------------------------------------
ppt_resources <- open_dataset(PATHIN) |>
  to_duckdb()
ppt_resources_monthly <- resource_count_monthly(ppt_resources, DATES)
write_parquet(ppt_resources_monthly, PATHOUT)
