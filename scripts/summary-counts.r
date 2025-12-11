################################################################################
# Summary counts
#
# This script computes summary counts for preprints, nodes, and users.
#
# Input tables:
# - osf_abstractnode (database)
# - osf_preprint (database)
# - osf_osfuser (database)
# Output file:
# - /data/registration.parquet
################################################################################

# Packages
library(arrow)
library(dbplyr)
library(duckplyr)
library(glue)
library(lubridate)
library(stringr)
library(tidyr)
library(purrr)

# Modules
box::use(
  R / connect[open_parquet],
  R / criteria[SPAM_DEF]
)

# Functions
get_n <- function(tbl) {
  tbl |>
    collect() |>
    nrow()
}


# Load
osfuser_tbl <- open_parquet(tbl = "osf_osfuser", duck = FALSE)
preprint_tbl <- open_parquet(tbl = "osf_preprint", duck = FALSE)
abstractnode_tbl <- open_parquet(tbl = "osf_abstractnode", duck = FALSE)


# Summary queries
query_reg <- abstractnode_tbl |>
  select(type, registered_from_id) |>
  filter(type == "osf.registration")
query_node <- abstractnode_tbl |>
  select(type, root_id) |>
  filter(type == "osf.node")

# Compute numbers
n_regs <- get_n(query_reg)
n_regs_from_node <- query_reg |>
  filter(!is.na(registered_from_id)) |>
  get_n()
n_regs_new <- n_regs - n_regs_from_node
# n_regs_new_chk <- query_reg |>
#   filter(is.na(registered_from_id)) |>
#   get_n()
# n_regs_new == n_regs_new_chk

n_ppts <- preprint_tbl$num_rows
n_users <- osfuser_tbl$num_rows

n_nodes <- get_n(query_node)
n_root_nodes <- query_node |>
  filter(root_id == id) |>
  get_n()
n_child_nodes <- n_nodes - n_root_nodes


# Inspect abstractnode
df <- collect(query_node)
