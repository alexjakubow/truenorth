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
  R / parameters[DATES],
  R / datamodel[open_parquet],
  R / helpers[tidyup]
)

# I/O
PQROOT <- "data/preprint"
PATHIN <- glue("{PQROOT}_supplements.parquet")
PATHOUT <- glue("{PQROOT}_supplements_tsmonthly.parquet")


# FUNCTIONS -----------------------------------------------------------------
ppt_supplement_status <- function(tbl, date, ...) {
  tbl |>
    dplyr::filter(
      supplement_status_date <= date
    ) |>
    dplyr::summarise(
      .by = c(...),
      n_supp_actions = n(),
      n_supp_add = sum(supplement_status == "add-supplement", na.rm = TRUE),
      n_supp_rmv = sum(supplement_status == "remove-supplement", na.rm = TRUE),
      n_supp_net = sum(supplement_status == "add-supplement", na.rm = TRUE) -
        sum(supplement_status == "remove-supplement", na.rm = TRUE),
    )
}

supplement_count_monthly <- function(tbl, dates) {
  purrr::map(
    dates,
    ~ ppt_supplement_status(
      tbl = tbl,
      date = .x,
      preprint_id
    ) |>
      collect(),
    .progress = TRUE
  ) |>
    tidyup(names_src = dates, names_to = "date") |>
    arrange(preprint_id, date)
}


# MAIN ----------------------------------------------------------------------
ppt_supps <- open_dataset(PATHIN) |>
  to_duckdb()
ppt_supps_monthly <- supplement_count_monthly(ppt_supps, DATES)
write_parquet(ppt_supps_monthly, PATHOUT)
