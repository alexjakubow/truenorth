# I/0 ----------------------------------------------------------------
#' Local path to directory of parquet files
#' @export
PARQUET_PATH <- "~/osfdata/parquet"


# CONSTANTS ----------------------------------------------------------------
#' @export
PLANS <- "prereg"

#' @export
OUTPUTS <- c("code", "data", "materials", "supplements")

#' @export
OUTCOMES <- "papers"

# TIME ----------------------------------------------------------------
TIMESPAN <- c("2018-01-01", "2025-12-01")
DELTA <- "month"

#' @export
DATES <- seq(as.POSIXct(TIMESPAN[1]), as.POSIXct(TIMESPAN[2]), by = DELTA)
