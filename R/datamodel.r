box::use(
  R/parameters[PARQUET_PATH],
)

# POSTGRES CONNECTIONS ---------------------------------------------------------
#' Define connection to OSF PostgreSQL database
#' @export
pg_connect <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    user = Sys.getenv("PGUSER"),
    host = Sys.getenv("PGHOST"),
    port = Sys.getenv("PGPORT"),
    dbname = Sys.getenv("PGDATABASE"),
  )
}

#' Stage a data model from OSF PostgreSQL database
#'
#' Bundles together `pg_connect()` and `dm::dm_from_con()` so that connection to PostgreSQL database and creation of the data model can be done in one step
#' @param keys Attempt to learn the keys from the database using `dm::dm_from_con()`.  Defaults to `TRUE`.
#' @export
stage_pg <- function(keys = TRUE) {
  pg <- pg_connect()
  dm::dm_from_con(pg, learn_keys = keyed)
}


# RELATIONAL KEY HELPERS -------------------------------------------------------
# NOTE: These are not exported, but are used internally by `stage_dm()`

#' Add a primary key to a data model
#'
#' @param dm Data model (`dm` object)
#' @param table Name of table to add primary key
#' @param pk_col Name of column to use as primary key
add_pk <- function(dm, table, pk_col) {
  tbl <- rlang::sym(table)
  col <- rlang::sym(pk_col)

  dm |>
    dm::dm_add_pk(table = !!tbl, columns = !!col)
}

#' Add foreign keys to a data model
#'
#' @param dm Data model (`dm` object)
#' @param child_table Name of the child table
#' @param child_fk_cols Name of the column(s) in the child table that reference the parent table
#' @param parent_table Name of the parent table
#' @param ... Additional arguments passed to `dm::dm_add_fk()`
add_fk <- function(dm, child_table, child_fk_cols, parent_table, col_name) {
  tbl <- rlang::sym(child_table)
  col <- rlang::sym(child_fk_cols)
  ref_tbl <- rlang::sym(parent_table)

  dm |>
    dm::dm_add_fk(table = !!tbl, columns = !!col, ref_table = !!ref_tbl)
}

#' Add primary and foreign keys to a data model
#'
#' @param dm Data model (`dm` object)
#' @param tbl_pk Data frame generated from `dm::dm_get_all_pks()`
#' @param tbl_fk Data frame generated from `dm::dm_get_all_fks()`
add_all_keys <- function(dm, tbl_pk, tbl_fk) {
  # Add primary keys
  for (i in 1:nrow(tbl_pk)) {
    dm <- add_pk(dm, tbl_pk$table[i], tbl_pk$pk_col[[i]])
  }

  # Add foreign keys
  for (i in 1:nrow(tbl_fk)) {
    dm <- add_fk(
      dm,
      tbl_fk$child_table[i],
      tbl_fk$child_fk_cols[[i]],
      tbl_fk$parent_table[i]
    )
  }
  return(dm)
}


# DUCKDB CONNECTIONS -----------------------------------------------------------
#' Stage OSF data model from a DuckDB database
#'
#' @param db Path to a DuckDB database.  Defaults to `Sys.getenv("DUCKDB_PATH")`.
#' @param keyfile Path to a relational key file RDS object.  Defaults to  `Sys.getenv("KEYFILE")`.
#' @param read_only Read-only connection. Defaults to `TRUE`.
#' @param keyed Apply relational keys from `keyfile`.  Defaults to `TRUE`.
#' @export
stage_dm <- function(
  db = Sys.getenv("DUCKDB_PATH"),
  keyfile = Sys.getenv("KEYFILE"),
  read_only = TRUE,
  keyed = TRUE
) {
  # Initialize connection
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = db, read_only = read_only))

  # Create data model
  duck_dm <- dm::dm_from_con(con, learn_keys = FALSE)

  # Add keys from keyfile
  if (keyed) {
    keys <- readRDS(keyfile)
    duck_dm <- add_all_keys(duck_dm, keys$primary_keys, keys$foreign_keys)
  }

  return(duck_dm)
}



# SUMMARY AND DIAGNOSTICS ------------------------------------------------------
#' Check constraints in data model
#' 
#' @param dm Data model (`dm` object)
#' @export
check_constraints <- function(dm) {
  result <- dm::dm_examine_constraints(dm)
  
  if (length(which(result$problem !="")) > 0) {
    print(result)
    return(FALSE)
  } else {
    return(TRUE)
  }
}


#' Get field names for each table in a data model
#' 
#' @param dm Data model (`dm` object)
#' @param tables Character vector of table names. Defaults to NULL.
#' @export
dm_get_cols <- function(dm, tables = NULL) {
  # Use all tables if no tables specified
  if (is.null(tables)) {
    TABLES <- names(dm)
  } else {
    TABLES <- tables
  }

  cols <- purrr::map(
    TABLES,
    ~ dm |>
        dm::dm_zoom_to(!!sym(.x)) |>
        colnames()
  )
  names(cols) <- TABLES

  return(cols)
}


#' Get number of rows for each table in a data model
#' @export
dm_get_nrows <- function(dm) {
  sort(dm::dm_nrow(dm))
}


#' Compare two data models
#' 
#' @param dm1 Data model 1 (`dm` object)
#' @param dm2 Data model 2 (`dm` object)
#' @param logger Logger object. Defaults to NULL, in which case a logger will be initialized at run time.
#' @export
compare_dms <- function(dm1, dm2) {
  message("Checking number of rows across all tables...")
  dm1_rows <- dm_get_nrows(dm1)
  mydm_rows <- dm_get_nrows(dm2)
  row_pass <- identical(dm1_rows, mydm_rows)
  if (row_pass) {
    message("Number of rows match!")
  } else {
    message("Number of rows do not match!")
  }

  message("Checking column names...")
  dm1_cols <- dm_get_cols(dm1)
  mydm_cols <- dm_get_cols(dm2)
  col_pass <- identical(dm1_cols, mydm_cols)
  if (col_pass) {
    message("Column names match!")
  } else {
    message("Column names do not match!")
  }
 
  if (row_pass & col_pass) {
    message("Data models match!")
    return(TRUE)
  } else {
    message("Data models do not match!")
    return(FALSE)
  }
}


# ARROW HELPERS ----------------------------------------------------------------
#' Open an OSF Parquet dataset
#'
#' @param dir Path to directory containing Parquet files.  Defaults `PARQUET_PATH`, defined in `R/parameters.r`.
#' @param tbl Name of Parquet file (i.e., OSF database table)
#' @param duck Logical. Should the dataset be converted to a DuckDB dataset using `duckdb::as_duckdb()`?  Defaults to `TRUE`.
#' @export
open_parquet <- function(dir = PARQUET_PATH, tbl, duck = TRUE) {
  if (duck) {
    arrow::open_dataset(file.path(dir, paste0(tbl, ".parquet"))) |>
      arrow::to_duckdb()
  } else {
    arrow::open_dataset(file.path(dir, paste0(tbl, ".parquet")))
  }
}


# DM HELPERS -------------------------------------------------------------------
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
