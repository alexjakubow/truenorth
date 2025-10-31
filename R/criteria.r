# This module defines open science criteria to be used throughout the project

#' SPAM formula
#'
#' This defines how spam is calculated for open science objects on OSF.  At present, it is a binary indicator, and is only awarded if the object is CONFIRMED_SPAM (i.e., spam value of 2).
#' @export
SPAM_DEF <- rlang::expr(is_spam == 2)


# Open Science Registrations
#' @export
CRITERIA_OSR <- rlang::exprs(
  "open" = visibility == "public",
  "nondeprecated" = !is.na(registered_date) &
    is.na(deleted) &
    retraction == "non-retracted",
  "authentic" = spam == "non-spam",
  # legacy/deprecated criteria
  "open_strict" = visibility == "public" & embargo == "unembargoed"
)

#' Open Science Preprints
#' @export
CRITERIA_OSP <- rlang::exprs(
  "open" = visibility == "public",
  "nondeprecated" = machine_state == "accepted" &
    !is.na(date_published) &
    is.na(date_withdrawn) &
    is.na(deleted),
  "authentic" = spam == "non-spam"
)
