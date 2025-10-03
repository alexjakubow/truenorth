TYPES <- c(
  preprint = "NP",
  registration = "NR",
  project = "NP",
  component = "NC",
  user = "U"
)

# Lookup an object's ID using its GUID
lookup_id <- function(dm, guid) {
  dm |>
    dm::dm_zoom_to(osf_guid) |>
    dplyr::select(id, `_id`) |>
    dplyr::filter(`_id` == guid) |>
    dplyr::pull(id)
}


# Lookup an object's GUID using its ID and type
lookup_id <- function(dm, guid) {
  dm |>
    dm::dm_zoom_to(osf_guid) |>
    dplyr::select(id, `_id`) |>
    dplyr::filter(`_id` == guid) |>
    dplyr::pull(id)
}

#' Generalized summary function for tibbles and data frames
#' @export
summarizer <- function(df, ...) {
  df |>
    dplyr::group_by(...) |>
    dplyr::summarize(
      n = dplyr::n()
    )
}

#' @export
create_uid <- function(tbl, id, type) {
  tbl |>
    dplyr::mutate(uid = paste0(TYPES[type], "_", {{ id }}))
}

# I/O HELPERS ------------------------------------------------------------------
#' Open and load a Parquet table from relative path to `data/`
#' @export
openup <- function(tbl) {
  open_parquet(dir = here::here("data"), tbl) |>
    dplyr::collect()
}


# GOOGLE HELPERS ---------------------------------------------------------------
#' Wrapper to authenticate with Google APIs
#' @export
google_auths <- function(user = Sys.getenv("GOOGLE_USER")) {
  googledrive::drive_auth(email = user)
  #googlesheets4::gs4_auth(token = googledrive::drive_auth())
}

#' Get Last Modified Timestamp
#'
#' @param file_id Google Drive File ID
#'
#' @returns Character string of last modified timestamp.
#' @export
get_last_modified <- function(file_id) {
  metadata <- googledrive::drive_get(id = file_id) |>
    purrr::pluck("drive_resource")

  metadata[[1]]$modifiedTime
}


# PURRR HELPERS ----------------------------------------------------------------
#' Generalized rbind for lists of data frames
#'
#' @param mapped_list List of data frames to rbind.
#' @param names_src Source vector of names.
#' @param names_to Column to bind names to.
#' @export
tidyup <- function(mapped_list, names_src, names_to) {
  mapped_list |>
    purrr::set_names(names_src) |>
    purrr::list_rbind(names_to = names_to)
}


# REGISTRATION HELPERS ---------------------------------------------------------
#' Tidy up registry names
#'
#' @export
tidy_registry_names <- function(x) {
  gsub(
    "Registry|Registries",
    "",
    gsub(
      "\\s+$",
      "",
      gsub("egap", "EGAP", x)
    )
  )
}

#' Tidy up template names
#'
#' @export
tidy_template_names <- function(x) {
  dplyr::case_when(
    x == "OSF Preregistration" ~ "OSF Prereg.",
    x == "Open-Ended Registration" ~ "Open-Ended Reg.",
    x == "Preregistration Template from AsPredicted.org" ~
      "AsPredicted.org Prereg.",
    x == "OSF-Standard Pre-Data Collection Registration" ~
      "OSF Pre-Data Collection Reg.",
    x == "Generalized Systematic Review Registration" ~
      "Systematic Review Reg.",
    x ==
      "Pre-Registration in Social Psychology (van 't Veer & Giner-Sorolla, 2016): Pre-Registration" ~
      "Social Psych. Prereg.",
    x == "Prereg Challenge" ~ "Prereg Challenge",
    x == "Secondary Data Preregistration" ~ "Secondary Data Prereg.",
    x == "Qualitative Preregistration" ~ "Qualitative Prereg.",
    x == "EGAP Registration" ~ "EGAP Reg.",
    x == "Registered Report Protocol Preregistration" ~
      "RegReport Protocol Prereg.",
    x == "Outdated EGAP Registration (DO NOT USE)" ~ "Outdated EGAP Reg.",
    x == "Character Lab Short-Form Registration" ~
      "Character Lab Short-Form Reg.",
    x == "Preregistration for Sample and Wave One Data" ~
      "GFS Sample/W1 Prereg.",
    x == "Preregistration for Waves 1 and 2 Data" ~ "GFS W1/W2 Prereg.",
    x == "Real World Evidence Recommended Minimum Study Registration Template" ~
      "RWE Recommended Min. Study Reg.",
    x == "Character Lab Long-Form Registration" ~
      "Character Lab Long-Form Reg.",
    x == "Election Research Preacceptance Competition" ~
      "Election Research Competition",
    x == "ASIST Hypothesis/Capability Registration" ~ "ASIST Hypothesis Reg.",
    x == "ASIST Hypothesis/Capability Preregistration" ~
      "ASIST Hypothesis Prereg.",
    x == "ASIST Results Registration" ~ "ASIST Results Reg.",
    x == "Hypothesis-Testing Studies Using YOUth Data" ~
      "Hypothesis Testing (YOUth)",
    x == "Data Archiving and Sharing Template" ~ "Data Archiving/Sharing",
    x == "Character Lab Registration" ~ "Character Lab Reg.",
    x == "Character Lab Registration " ~ "Character Lab Reg.",
    x == "Lifecycle Journal Submission" ~ "Lifecycle Journal",
    x == "EGAP Project" ~ "EGAP Project",
    x == "Platform Registration" ~ "Platform Reg.",
    x == "Other studies using YOUth data" ~ "Other Studies (YOUth)",
    x == "Confirmatory - General" ~ "Confirmatory",
    x == "Services Environment/Application Package" ~ "Services",
    x == "Services Registration" ~ "Services",
    x == "Services" ~ "Services",
    .default = "Other"
  )
}


#' @export
tidy_status <- function(x) {
  dplyr::case_when(
    x == "n_los_plan" ~ "Open Sci. Registrations (OSR)",
    x == "n_los_outcomes" ~ "OSR + OS Outcome(s)",
    x == "n_los_outputs" ~ "OSR + OS Output(s)",
    x == "n_los_full" ~ "LOS-Reg"
  )
}

#' @export
tidy_table <- function(tbl, ...) {
  tbl |>
    dplyr::group_by(...) |>
    dplyr::select(n_los_plan, n_los_outcomes, n_los_outputs, n_los_full) |>
    tidyr::pivot_longer(
      cols = starts_with("n_"),
      names_to = "Status",
      values_to = "N",
      values_drop_na = FALSE
    ) |>
    dplyr::group_by(...) |>
    dplyr::mutate(
      Status = tidy_status(Status)
    ) |>
    dplyr::ungroup()
}
