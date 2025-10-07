# I/0 ----------------------------------------------------------------
#' Local path to directory of parquet files
#' @export
PARQUET_PATH <- "~/osfdata/parquet"


# TIME ----------------------------------------------------------------
TIMESPAN <- c("2018-01-01", "2025-10-01")
DELTA <- "month"

#' @export
DATES <- seq(as.POSIXct(TIMESPAN[1]), as.POSIXct(TIMESPAN[2]), by = DELTA)
