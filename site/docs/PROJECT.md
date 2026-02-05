# Estuary Flow Documentation

Documentation for the Estuary Flow real-time data platform.

## Goals

- [x] Add document metadata fields (`_meta`) documentation
- [x] Add feature flags documentation for captures and materializations
- [x] Add custom column types documentation (`castToString`, `DDL`)
- [x] Add schema evolution troubleshooting guide
- [x] Reorganize advanced docs under Using Estuary > Advanced Usage
- [ ] Merge PR #2648 with all documentation updates (blocked by unrelated CI failure - sccache issue)

## Plan

### Phase 1: Core Documentation Pages (Complete)
- [x] Create metadata-fields.md explaining `_meta` object structure
- [x] Create feature-flags.md documenting connector flags
- [x] Create custom-column-types.md for field type overrides
- [x] Add explicit JSON paths for programmatic spec modification

### Phase 2: Troubleshooting (Complete)
- [x] Create troubleshooting section under guides/
- [x] Add schema-inference-issues.md
- [x] Expand schema-evolution.md with common issues
- [x] Add documentation targets for error message improvements

### Phase 3: Organization (Complete)
- [x] Move pages from features/ to guides/advanced-usage/
- [x] Fix internal links after reorganization
- [x] Verify build passes

## Inbox

- [2026-02-04] Update OpenMetrics API Documentation (site/docs/features/openmetrics-api.md): Add "How metrics are calculated" section explaining month-to-date counters, add Time Scope column to metrics table, add "Using with Prometheus/Grafana" subsection with increase() examples and month boundary warnings, clarify in Custom integration that API returns month-to-date totals not deltas
- Consider adding more connector-specific troubleshooting pages
- Document additional feature flags as they're added to connectors

## Notes

- Docs site has a pre-existing HubSpot redirect build error (unrelated to our changes)
- PR #2648 contains all documentation work from this project
