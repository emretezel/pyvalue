# Local Development

## Environment

Use the `pyvalue` conda environment for project work.

Example install:

```bash
python -m pip install -e .[dev]
conda activate pyvalue
```

## Project Layout

Main repo areas:
- `src/pyvalue/`: application code
- `tests/`: pytest suite
- `screeners/`: YAML screen definitions
- `data/`: local database, logs, and output artifacts
- `private/`: local credentials and config
- `docs/`: project documentation

Main application subsystems in `src/pyvalue/`:
- `universe/`
- `ingestion/`
- `normalization/`
- `marketdata/`
- `metrics/`
- `storage.py` and `migrations.py`
- `cli.py`

## Common Development Workflows

Run tests:

```bash
conda run -n pyvalue pytest
```

Run formatting and checks:

```bash
conda run -n pyvalue ruff format .
conda run -n pyvalue ruff check .
conda run -n pyvalue mypy src/pyvalue
```

## Adding New Metrics

Typical workflow:
1. confirm normalized inputs exist
2. add or extend normalization only if needed
3. implement the metric in `src/pyvalue/metrics/`
4. register it in `src/pyvalue/metrics/__init__.py`
5. add or update tests
6. update `docs/reference/metrics.md`

## Documentation Expectations

When behavior changes:
- update the relevant docs page under `docs/`
- avoid keeping the same explanation in multiple files
- keep the root README short and link to the canonical topic page
