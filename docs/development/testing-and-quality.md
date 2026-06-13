# Testing and Quality Checks

## Required Commands

Run these in the `pyvalue` conda environment:

```bash
conda run -n pyvalue ruff format .
conda run -n pyvalue ruff check .
conda run -n pyvalue mypy src/ tests/
conda run -n pyvalue pytest
```

## What Each Check Covers

- `ruff format`: code formatting
- `ruff check`: linting and style problems
- `mypy`: strict static typing on **both** `src/` and `tests/` — `[tool.mypy]` in
  `pyproject.toml` enables `disallow_untyped_defs` and `check_untyped_defs`, so every
  function (tests included) must be fully annotated and have its body type-checked.
  Third-party stubs (`types-requests`, `types-PyYAML`) are dev dependencies, and the
  package ships a PEP 561 `py.typed` marker, so no `# type: ignore[import-untyped]` is
  needed. Do not silence errors with `# type: ignore` or `Any`/`object` widening — fix
  the types.
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
