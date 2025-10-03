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
  R / database[last_logged_action]
)

# I/O
PQROOT <- "data/preprint"
PQSUFFIX <- "tsmonthly.parquet"
PATH_ALL <- glue("{PQROOT}_{PQSUFFIX}")
PATH_PRV <- glue("{PQROOT}_providers_{PQSUFFIX}")


# DATA PREP ----------------------------------------------------------------
# Status datasets
pp_main <- open_parquet("data", "preprint")
pp_visibility <- open_parquet("data", "preprint_visibility")
pp_spam <- open_parquet("data", "preprint_spam")

# Combine statuses
pptime_tbl <- pp_main |>
  select(
    preprint_id,
    created,
    date_withdrawn,
    date_published,
    deleted,
    registry,
    has_data_links,
    has_prereg_links
  ) |>
  left_join(pp_visibility, by = "preprint_id", relationship = "many-to-many") |>
  left_join(pp_spam, by = "preprint_id", relationship = "many-to-many")


# FUNCTIONS ----------------------------------------------------------------
# Generate the status of each registration along criteria of interest at a given date
create_status_table <- function(date) {
  # Prep data sources
  snapshot <- pptime_tbl |>
    filter(created <= date)
  status_main <- snapshot |>
    select(
      preprint_id,
      created,
      date_withdrawn,
      date_published,
      machine_state,
      deleted
    ) |>
    distinct(preprint_id, .keep_all = TRUE)
  status_vis <- snapshot |>
    last_logged_action(
      visibility_status_date,
      visibility_status,
      date,
      preprint_id
    ) |>
    rename(visibility = status)
  status_spam <- snapshot |>
    last_logged_action(spam_status_date, spam_status, date, preprint_id) |>
    rename(spam = status)

  # Compute status
  status_at_date <- status_main |>
    left_join(status_vis, by = "preprint_id") |>
    left_join(status_spam, by = "preprint_id") |>
    mutate(
      is_open = ifelse(visibility == "public", 1, 0),
      is_nondeprecated = ifelse(
        !is.na(date_withdrawn) &
          !is.na(date_published) &
          is.na(deleted) &
          machine_state == "accepted",
        1,
        0
      ),
      is_authentic = ifelse(spam == "non-spam", 1, 0)
    )
  return(status_at_date)
}

#' Compute the lifecycle open status of each preprint at a given date
compute_los_status <- function(date, method = c("typed", "summed")) {
  # Get statuses
  status_at_date <- create_status_table(date)

  # Get badges
  badges_at_date <- reg_badge_status(
    reg_badges,
    date,
    node_id,
    artifact_type
  )
  # Apply lifecycle open science recipe to get badges
  if (method[1] == "typed") {
    recipe_at_date <- badges_at_date |>
      reg_recipe_status_typed()
  }
  if (method[1] == "summed") {
    recipe_at_date <- badges_at_date |>
      reg_recipe_status_summed()
  }

  # Join
  status_at_date |>
    left_join(recipe_at_date, by = "node_id") |>
    mutate(
      n_outputs = ifelse(is.na(n_outputs), 0, n_outputs),
      n_outcomes = ifelse(is.na(n_outcomes), 0, n_outcomes)
    )
}

#' Summarize the lifecycle open status all registration at a given date
los_summary <- function(status_tbl, ...) {
  status_tbl |>
    summarise(
      .by = c(...),
      n_total = n(),
      # Openness components
      n_public = sum(visibility == "public"),
      n_not_embargoed = sum(embargo == "unembargoed"),
      # Non-deprecated components
      n_registered = sum(!is.na(registered_date)),
      n_not_deleted = sum(!is.na(deleted)),
      n_not_retracted = sum(retraction == "non-retracted"),
      # LOS Plan (i.e., open, non-deprecated, and authentic)
      n_open = sum(is_open == 1),
      n_not_deprecated = sum(is_nondeprecated == 1),
      n_authentic = sum(is_authentic == 1),
      n_open_notdep = sum(is_open == 1 & is_nondeprecated == 1),
      n_open_auth = sum(is_open == 1 & is_authentic == 1),
      n_notdep_auth = sum(is_authentic == 1 & is_nondeprecated == 1),
      # Full LOS
      n_los_plan = sum(
        is_open == 1 & is_nondeprecated == 1 & is_authentic == 1,
        na.rm = TRUE
      ),
      # LOS Relationships
      n_los_outcomes = sum(n_outcomes > 0, na.rm = TRUE),
      n_los_outputs = sum(n_outputs > 0, na.rm = TRUE),
      # Full LOS
      n_los_complete = sum(
        is_open == 1 &
          is_nondeprecated == 1 &
          is_authentic == 1 &
          n_outcomes > 0 &
          n_outputs > 0,
        na.rm = TRUE
      )
    )
}

# Wrapper to compute summaries for all dates
compute_all_summaries <- function(
  dates,
  grp = NULL,
  method = c("typed", "summed")
) {
  purrr::map(
    dates,
    ~ compute_los_status(.x, method) |>
      los_summary({{ grp }}) |>
      collect(),
    .progress = TRUE
  ) |>
    tidyup(dates, "date")
}
