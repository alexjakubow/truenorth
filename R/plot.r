# DATA PREP ----------------------------------------------------------------
#' Pivot the data to long format for plotting
#'
#' @param tbl The data to pivot
#' @param ... Column names to group by
#' @export
pivoter <- function(tbl, ...) {
  tbl |>
    dplyr::select(date, ..., starts_with("n_")) |>
    dplyr::group_by(...) |>
    tidyr::pivot_longer(
      cols = dplyr::starts_with("n_"),
      names_to = "criteria",
      names_prefix = "n_",
      values_to = "n",
      values_drop_na = FALSE
    )
}

#' Factorize object states for plotting
#'
#' @param tbl Table to factorize
#' @param col Column to (re)factor
#' @param coarseness Coarsening level. 1 = most detailed, 2 = criteria-level, 3 = multiple-criteria-level. Defaults to 1.
#' @export
factorizer <- function(tbl, col, coarseness = c(1, 2, 3), ...) {
  # Most detailed
  if (coarseness[1] == 1) {
    LVLS <- c(
      "total",
      "public",
      "not_embargoed",
      "registered",
      "not_deleted",
      "not_retracted",
      "not_spam"
    )
    tbl_out <- tbl |>
      dplyr::filter({{ col }} %in% LVLS) |>
      dplyr::group_by(...) |>
      dplyr::mutate(
        {{ col }} := factor(
          {{ col }},
          levels = c(
            "total",
            "public",
            "not_embargoed",
            "registered",
            "not_deleted",
            "not_retracted",
            "not_spam"
          )
        )
      )
  }

  # Coarsening
  if (coarseness[1] > 1) {
    LVLS <- c(
      "total",
      "open",
      "not_deprecated",
      "authentic",
      "open_notdep",
      "open_auth",
      "notdep_auth",
      "los_plan"
    )
    tbl_out <- tbl |>
      dplyr::filter({{ col }} %in% LVLS) |>
      dplyr::group_by(...) |>
      dplyr::mutate(
        {{ col }} := dplyr::case_when(
          {{ col }} == "total" ~ "Total",
          {{ col }} == "open" ~ "Open",
          {{ col }} == "not_deprecated" ~ "Non-deprecated",
          {{ col }} == "authentic" ~ "Authentic",
          {{ col }} == "open_notdep" ~ "Open + Non-deprecated",
          {{ col }} == "open_auth" ~ "Open + Authentic",
          {{ col }} == "notdep_auth" ~ "Non-deprecated + Authentic",
          {{ col }} == "los_plan" ~ "Open Science Registration",
          .default = NA
        )
      )

    # Level-2 coarsening
    if (coarseness[1] == 2) {
      tbl_out <- tbl_out |>
        dplyr::mutate(
          {{ col }} := factor(
            {{ col }},
            levels = c(
              "Total",
              "Open",
              "Non-deprecated",
              "Authentic",
              "Open + Non-deprecated",
              "Open + Authentic",
              "Non-deprecated + Authentic",
              "Open Science Registration"
            )
          )
        )
    }

    # Level-3 coarsening
    if (coarseness[1] == 3) {
      LVS <- c(
        "Total",
        "Open + Non-deprecated",
        "Open + Authentic",
        "Open Science Registration"
      )
      tbl_out <- tbl_out |>
        dplyr::filter({{ col }} %in% LVS) |>
        dplyr::mutate(
          {{ col }} := factor({{ col }}, levels = LVS)
        )
    }
  }

  return(dplyr::ungroup(tbl_out))
}


#' Prepare data for time series plots
#'
#' Convert date to `Date` and calculate share by dividing by value of `n` at `total_category`
#'
#' @param tbl Table to prepare
#' @param total_category Total category
#'
#' @export
ts_prep <- function(tbl, total_category = "total", ...) {
  tbl |>
    dplyr::mutate(
      date = as.Date(date, "%Y-%m-%d")
    )
  # totals <- tbl |>
  #   dplyr::filter(criteria == total_category) |>
  #   dplyr::select(date, n_total = n, ...)
  # tbl |>
  #   left_join(totals, by = c("date", ...)) |>
  #   mutate(
  #     date = as.Date(date, "%Y-%m-%d"),
  #     share = n / n_total
  #   ) |>
  #   select(-n_total)
}
