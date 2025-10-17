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
source(file.path(PATH, "registration-ts.r")) # Monthly TS summaries + current status
source(file.path(PATH, "registration-ttlos.r")) # Time-to-event dataset
