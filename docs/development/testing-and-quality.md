# Testing and Quality Checks

## Required Commands

Run these in the `pyvalue` conda environment:

```bash
conda run -n pyvalue ruff format .
conda run -n pyvalue ruff check .
conda run -n pyvalue mypy src/pyvalue
conda run -n pyvalue pytest
```

## What Each Check Covers

- `ruff format`: code formatting
- `ruff check`: linting and style problems
- `mypy`: static typing on `src/pyvalue`
- `pytest`: unit and integration coverage

## Testing Expectations

When implementing a new feature:
- add or update unit tests
- add CLI-level coverage when behavior is visible through the CLI
- add normalization tests when new normalized concepts or fallback rules are introduced
- update docs when public behavior changes

## Metric-Specific Testing

For new metrics, cover:
- happy path
- missing-data behavior
- fallback behavior
- recency behavior
- currency mismatch or incompatibility behavior where relevant
- registry presence

## Documentation Checks

For doc-only changes, verify:
- links resolve to real files
- moved content exists in exactly one canonical location
- the root README still gets a new user to a working path quickly
