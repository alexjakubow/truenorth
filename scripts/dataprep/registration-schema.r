# Packages
library(arrow)
library(dplyr)
library(duckdb)
library(glue)
library(stringr)

# Modules
box::use(
  R / connect[open_parquet]
)

# I/O
PQROOT <- "data/registration"
PATHOUT <- glue("{PQROOT}_template.parquet")


# PREP REGISTRATIONS -----------------------------------------------------------
# Registration metadata from osf_abstractnode
reg_meta <- open_parquet(tbl = "osf_abstractnode") |>
  filter(type == "osf.registration") |>
  select(node_id = id, registered_meta)


# GET TEMPLATE IDS -------------------------------------------------------------
# Parse template IDs from `registered_meta`
reg_template_ids <- reg_meta |>
  filter(!is.na(registered_meta)) |>
  collect() |>
  mutate(template_id = str_extract(registered_meta, "[0-9][0-9a-z]{5,}")) |>
  select(-registered_meta)

# Prepare schema table
reg_schema <- open_parquet(tbl = "osf_registrationschema") |>
  select(
    schema_id = id,
    template_id = `_id`,
    template = name,
    schema_version,
    schema_active = active,
    schema_visitble = visible
  ) |>
  collect()


# CREATE TABLE ----------------------------------------------------------------
reg_template <- reg_template_ids |>
  left_join(reg_schema, by = "template_id")
write_parquet(reg_template, PATHOUT)

# # DEPRECATED -----------------------------------------------------------------
# # Registration schema with providers
# tbl_regprovider <- open_parquet(tbl = "osf_registrationschema_providers") |>
#   select(-id) |>
#   rename(
#     schema_id = registrationschema_id,
#     provider_id = registrationprovider_id
#   )

# # DIAGNOSTICS -----------------------------------------------------------------
# df <- read_parquet(PATH_REG_SMA)

# # Row counts
# FILTER_CONDITIONS <- rlang::exprs(
#   "valid_schema" = !is.na(schema_id),
#   "active_schema" = schema_active == TRUE,
#   "visible_schema" = schema_visitble == TRUE,
#   "valid_provider" = !is.na(provider_id),
#   "valid_version" = !is.na(schema_version),
#   "valid_active_visible" = !is.na(schema_id) &
#     schema_active == TRUE &
#     schema_visitble == TRUE
# )

# row_counts <- purrr::map(
#   FILTER_CONDITIONS,
#   ~ df |>
#     filter(!!.x) |>
#     nrow()
# )

# # List of schemas that are not visible
# df |>
#   filter(schema_visitble == FALSE) |>
#   distinct(template)

# # Profile by node
# df_n <- df |>
#   group_by(node_id) |>
#   summarise(
#     n_versions = n_distinct(schema_version),
#     n_schemas = n_distinct(schema_id),
#     n_providers = n_distinct(provider_id),
#     n_schema_versions = n_distinct(schema_id, schema_version, na.rm = TRUE)
#   )

# # Plots
# plot(df_n$n_versions)
# plot(df_n$n_schemas)
# plot(df_n$n_providers)
# plot(df_n$n_schema_versions)

# # NOTES -----------------------------------------------------------------------
# # It would appear that a registration can belong to multiple providers
# # Only 1 invisible schema: ASIST Hypothesis/Capability Preregistration
