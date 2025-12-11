box::use(
  R /
    criteria[
      CRITERIA_OSR,
      NDE_CATEGORY_OUTPUTS,
      NDE_CATEGORY_OUTCOMES,
      NDE_CATEGORY_PLANS
    ]
)

# DATABASE QUERIES -------------------------------------------------------------
#' Last logged action query
#'
#' @param tbl Database table connection to query (duckdb)
#' @param date_col Date column name
#' @param status_col Status column name
#' @param date Query date
#' @param ... Column names to group by
#' @export
last_logged_action <- function(tbl, date_col, status_col, date, ...) {
  tbl |>
    dplyr::filter({{ date_col }} <= date) |>
    dbplyr::window_order({{ date_col }}) |>
    dplyr::summarise(
      .by = c(...),
      status = last({{ status_col }})
    )
}


#' First logged action query
#'
#' @param tbl Database table connection to query (duckdb)
#' @param date_col Date column name
#' @param status_col Status column name
#' @param date Query date
#' @param ... Column names to group by
#' @export
first_logged_action <- function(tbl, date_col, status_col, date, ...) {
  tbl |>
    dplyr::filter({{ date_col }} <= date) |>
    dbplyr::window_order({{ date_col }}) |>
    dplyr::summarise(
      .by = c(...),
      status = first({{ status_col }})
    )
}


#' Cumulative count logged action query
#' @export
cumulative_logged_actions <- function(
  tbl,
  date_col,
  action_col,
  action_value,
  date,
  ...
) {
  tbl |>
    dplyr::filter({{ date_col }} <= date) |>
    dplyr::summarise(
      .by = c(...),
      n = sum({{ action_col }} == action, na.rm = TRUE)
    )
}


# REGISTRATION QUERIES ---------------------------------------------------------
#' Filters the registration_badges table for finalized artifacts that were:
#' 1. Created on/before the specified date
#' 2. Not deleted or deleted after the specified date.
#'
#' And then counts the number of artifacts by type and optional grouping variables.
#'
#' @param tbl Registration badges table
#' @param date Query date
#' @param ... Column names to group by
#'
#' @export
reg_badge_status <- function(tbl, date, ...) {
  tbl |>
    dplyr::filter(
      created <= date,
      deleted > date | is.na(deleted),
      finalized == TRUE
    ) |>
    dplyr::summarise(
      .by = c(...),
      n_artifacts = n(),
      n_data = sum(artifact_type == "data", na.rm = TRUE),
      n_materials = sum(artifact_type == "materials", na.rm = TRUE),
      n_code = sum(artifact_type == "code", na.rm = TRUE),
      n_supplements = sum(artifact_type == "supplements", na.rm = TRUE),
      n_papers = sum(artifact_type == "papers", na.rm = TRUE)
    )
}


#' Calculate the number of linked outcomes and outputs for each registraiton by summing on `artifact_type`
#' @export
reg_recipe_status_typed <- function(tbl) {
  tbl |>
    dplyr::summarise(
      .by = node_id,
      n_outputs = sum(
        artifact_type %in% c("data", "materials", "code", "supplements"),
        na.rm = TRUE
      ),
      n_outcomes = sum(artifact_type == "papers", na.rm = TRUE)
    )
}


#' Calculate the number of linked outcomes and outputs for each registraiton by summing across computed counts for each badge type (i.e.g, data, materials, code, supplements, papers
#'
#' @export
reg_recipe_status_summed <- function(tbl) {
  tbl |>
    dplyr::summarise(
      .by = node_id,
      n_outputs = sum(
        n_data + n_materials + n_code + n_supplements,
        na.rm = TRUE
      ),
      n_outcomes = sum(n_papers, na.rm = TRUE)
    )
}

# OSF NODE/PROJECT QUERIES -----------------------------------------------------
#' Filters the node table for component categories that were:
#' 1. Created on/before the specified date
#' 2. Not deleted or deleted after the specified date.
#'
#' And then counts the number of component categories by type and optional grouping variables.
#'
#' @param tbl Node table
#' @param date Query date
#' @param ... Column names to group by
#'
#' @export
nde_category_status <- function(tbl, date = Sys.Date(), ...) {
  tbl |>
    arrow::to_duckdb() |>
    dplyr::filter(
      created <= date,
      deleted > date | is.na(deleted)
    ) |>
    arrow::to_arrow() |>
    dplyr::summarise(
      .by = c(...),
      n_components = n(),
      # Plan components
      n_hypothesis = sum(category == "hypothesis"),
      n_procedure = sum(category == "procedure"),
      # Output components
      n_analysis = sum(category == "analysis"),
      n_data = sum(category == "data"),
      n_other = sum(category == "other"),
      n_methods = sum(category == "methods and measures"),
      n_software = sum(category == "software"),
      # Outcome components
      n_communication = sum(category == "communication"),
      # LOS components
      n_plans = sum(category %in% NDE_CATEGORY_PLANS),
      n_outputs = sum(category %in% NDE_CATEGORY_OUTPUTS),
      n_outcomes = sum(category %in% NDE_CATEGORY_OUTCOMES)
    ) |>
    dplyr::mutate(
      is_los = n_plans > 0 & n_outputs > 0 & n_outcomes > 0
    )
}


# OSF PREPRINTS ----------------------------------------------------------------
#' Filters the preprint_resources table for linked resources that were:
#' 1. Created on/before the specified date (set as "available")
#' and then counts the number of artifacts by type and optional grouping variables.
#'
#' @param tbl Preprint resourcs table
#' @param date Query date
#' @param ... Column names to group by
#'
#' @export
ppt_resource_status <- function(tbl, date, ...) {
  tbl |>
    dplyr::filter(
      created <= date
    ) |>
    dplyr::summarise(
      .by = c(...),
      n_resources = n(),
      n_prereg = sum(resource_type == "prereg", na.rm = TRUE),
      n_data = sum(resource_type == "data", na.rm = TRUE)
    )
}


ppt_supplement_status <- function(tbl, date, ...) {
  tbl |>
    dplyr::filter(
      created <= date
    ) |>
    dplyr::summarise(
      .by = c(...),
      n_supplements = n(),
      n_added = sum(resource_type == "supplement", na.rm = TRUE)
    )
}


#' Calculate the number of linked outcomes and outputs for each registraiton by summing on `resource_type`
#' @export
ppt_recipe_status_typed <- function(tbl) {
  tbl |>
    dplyr::summarise(
      .by = preprint_id,
      n_plans = sum(resource_type == "prereg", na.rm = TRUE),
      n_outputs = sum(resource_type == "data", na.rm = TRUE),
    )
}


#' Calculate the number of linked outcomes and outputs for each preprint by summing across computed counts for each resource type (i.e., data or prereg)
#' @export
ppt_recipe_status_summed <- function(tbl) {
  tbl |>
    dplyr::summarise(
      .by = preprint_id,
      n_plans = sum(n_prereg, na.rm = TRUE),
      n_outputs = sum(n_data, na.rm = TRUE)
    )
}
