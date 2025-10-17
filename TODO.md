# In Progress
- [/] Time from registration to LOS-Reg

# Backlog
## Analyses
- [ ] Revise preprints recipe

## Documentation
- [ ] Update README

## Workflow
- [ ] Processing function for `registration-events.r` to iterate for a paticular bundle of actions and criteria

## Data
- [ ] Flag state (active vs. inactive) registries and templates

## UI
- [ ] Transition to dashboard

# Completed
- [x] Overview of data generation process (from postgres to parquet via R/DuckDB)  (available [here](https://github.com/alexjakubow/archer))
- [x] Update with more recent backup
- [x] Sortable tables
- [x] Fix inconsistent navigation headers
- [x] Create template dataset
- [x] Create template-registry dataset
- [x] Confirm only 1 schema per registration
- [x] Correct use of terminology
- [x] Document order of operations for code execution
- [x] Consolidate data processing and presentation layers into single repository
- [x] Refactor `registrations.r` into two separate files for permanent features and log-based features.