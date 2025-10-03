# In Progress
- [ ] Update with more recent backup
- [ ] Time from registration to LOS-Reg

# Backlog
## Documentation
- [ ] Update README
- [x] Correct use of terminology
- [x] Document order of operations for code execution
- [ ] Overview of data generation process (from postgres to parquet via R/DuckDB)

## Workflow
- [x] Consolidate data processing and presentation layers into single repository
- [ ] Processing function for `registration-events.r` to iterate for a paticular bundle of actions and criteria
- [x] Refactor `registrations.r` into two separate files for permanent features and log-based features.

## Data
- [ ] Flag state (active vs. inactive) registries and templates
- [x] Create template dataset
- [x] Create template-registry dataset
- [x] Confirm only 1 schema per registration

## UI
- [x] Fix inconsistent navigation headers
- [ ] Transition to dashboard