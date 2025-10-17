options(renv.config.pak.enabled = TRUE)
source("renv/activate.R")

# Copy R files to repo from local source modules (if necessary)
if (Sys.info()["user"] == "alexjakubow") {
  BOXPATH <- Sys.getenv("R_BOX_PATH")

  # Create local backup of ./R folder (just in case)
  fs::dir_create(here::here("R2D2"))
  if (fs::dir_exists(here::here("R"))) {
    fs::dir_copy(here::here("R"), here::here("R2D2"), overwrite = TRUE)
  }

  # Copy to repo for reproducibility
  mods <- list.files(
    BOXPATH,
    pattern = ".r",
    full.names = TRUE,
    recursive = TRUE
  )
  local_files <- fs::path_file(mods)
  local_subdirs <- gsub("^/", "", gsub(BOXPATH, "", fs::path_dir(mods)))
  repo_paths <- file.path(here::here(), local_subdirs, local_files)
  for (i in seq_along(mods)) {
    fs::dir_create(local_subdirs[i])
    fs::file_copy(mods[i], repo_paths[i], overwrite = TRUE)
  }
}

# Reset R_BOX_PATH to repo
Sys.setenv(R_BOX_PATH = here::here())
