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

# I/O
PQROOT <- "data/preprint"
PATHOUT <- "data/preprint_assertions.parquet"


# HELPER FUNCTIONS ----------------------------------------------------------
assign_assertion <- function(x) {
  dplyr::case_when(
    x == "no" ~ 0,
    x == "not_applicable" ~ 0,
    x == "available" ~ 1
  )
}


# CORE TABLES ---------------------------------------------------------------
pp_tbl <- open_parquet("data", "preprint")
pplog_tbl <- open_parquet(tbl = "osf_preprintlog")


# PREP ----------------------------------------------------------------------
# Reduce tables
pp_tbl <- pp_tbl |>
  select(preprint_id)
planadd_tbl <- pplog_tbl |>
  filter(action == "has_prereg_links_updated") |>
  select(preprint_id, params)


# Link preprints to preregistration via parsed `params` field in log
plan_adds <- pp_tbl |>
  inner_join(planadd_tbl, by = "preprint_id") |>
  collect() |>
  mutate(
    params = str_extract(params, pattern = ("no|not_applicable|available"))
  ) |>
  rename(artifact_id = params) |>
  left_join(collect(outcomeartifact_tbl), by = "artifact_id") |>
  select(-artifact_id) |>
  mutate(
    artifact_type = assign_assertion(artifact_type)
  )


# EXPORT --------------------------------------------------------------------
reg_adds |>
  write_parquet(PATHOUT)
