#' Pull and collect a table from a data model
#'
#' Wrapper around `dm::pull_tbl()` and `dm::collect()` so we don't have to call them separately.
#'
#' @param tbl Table to pull and collect
#' @export
pull_collect <- function(tbl) {
  tbl |>
    dm::pull_tbl() |>
    dm::collect()
}
