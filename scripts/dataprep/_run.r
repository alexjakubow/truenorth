################################################################################
# Run Data Prep Scripts
#
# This script runs all data prep scripts.  It is intended to be run from the
# root of the repository and in the sequence listed below.
################################################################################

library(here)
PATH <- file.path(here(), "scripts", "dataprep")


# REGISTRATION-RESOURCES -------------------------------------------------------
# Detailed tables
source(file.path(PATH, "registration-schema.r"))

# Core tables
source(file.path(PATH, "registration.r"))
source(file.path(PATH, "registration-log.r"))

# Lifecycle events
source(file.path(PATH, "registration-events.r"))
source(file.path(PATH, "registration-badges.r"))

# Analysis datasets
source(file.path(PATH, "registration-ts.r")) # Monthly TS summaries
source(file.path(PATH, "registration-badges-ttlos.r")) # Time-to-event dataset


# PREPRINT-RESOURCES -----------------------------------------------------------
# Core tables
source(file.path(PATH, "preprint.r"))
source(file.path(PATH, "preprint-log.r"))

# Lifecycle events
source(file.path(PATH, "preprint-events.r"))
source(file.path(PATH, "preprint-resources.r"))
source(file.path(PATH, "preprint-supplements-ts.r"))

# Analysis datasets
source(file.path(PATH, "preprint-ts.r"))
source(file.path(PATH, "preprint-resources-ttlos.r")) # Time-to-event dataset

# NODE ----------------------------------------------------------------------
# # source(file.path(PATH, "nodetype.r"))

# # Core tables
# source(file.path(PATH, "node.r"))
# source(file.path(PATH, "node-log.r"))

# # Lifecycle events
# source(file.path(PATH, "node-events.r"))
# source(file.path(PATH, "node-categories.r"))

# # Analysis datasets
# source(file.path(PATH, "node-ts.r"))
# source(file.path(PATH, "node-categories-ttlos.r")) # Time-to-event dataset
