# Repository Guidelines

## Project Description
pyvalue is a Python toolkit for ingesting, normalizing, and screening fundamental market data with a focus on value-investing workflows. It supports SEC (US-only) and EODHD data sources, stores results in SQLite, and exposes a CLI for universe loading, metric computation, and screening.

## Project Structure & Module Organization
- `src/pyvalue/`: core package. `ingestion/`, `normalization/`, `marketdata/`, `metrics/`, and `universe/` hold domain logic; `cli.py` wires the CLI; `storage.py` and `migrations.py` manage SQLite.
- `tests/`: pytest suite.
- `screeners/`: YAML screen definitions (e.g., `screeners/value.yml`).
- `data/`: local artifacts (`data/pyvalue.db`, `data/screen_results_*.csv`) and bundled FX CSVs.
- `private/`: local config and API keys (`private/config.toml`, gitignored).

## Build, Test, and Development Commands
- `python -m pip install -e .[dev]`: install editable package (Python >=3.9).
- Use the `pyvalue` conda environment for all project commands.
- `conda run -n pyvalue pytest`: run the test suite in the `pyvalue` conda env.
- `pyvalue --help`: list CLI entry points; examples in `README.md` show workflows.

## Coding Style & Naming Conventions
- Use 4-space indentation and standard Python conventions: `snake_case` for modules/functions, `CapWords` for classes, and `UPPER_SNAKE_CASE` for constants.
- Keep type hints and docstrings where they already exist; follow patterns in `src/pyvalue/cli.py`.
- Format with `ruff format` and keep diffs consistent with surrounding code.
- New Python modules should start with a module docstring using triple quotes (`""" ... """`) that briefly describes the module and lists the author name.
- Keep Python modules well commented, especially around non-obvious logic or business rules.

## Testing Guidelines
- Framework: `pytest` (see `pyproject.toml`).
- Use the `pyvalue` conda environment for tests; run `conda run -n pyvalue pytest`.
- Naming: files `tests/test_*.py`, functions `test_*`.
- Add tests for new metrics, CLI commands, and normalization paths; reuse fixtures in `tests/conftest.py`.
- When implementing new features, always add or update unit tests.

## Formatting & Static Checks
- After every Codex update, run these in the `pyvalue` conda env:
  - `conda run -n pyvalue ruff format .`
  - `conda run -n pyvalue ruff check .`
  - `conda run -n pyvalue mypy src/pyvalue`
- If any command reports formatting or static-check errors, fix them automatically.

## Commit & Pull Request Guidelines
- Commit messages in history are concise, imperative, and often scoped (e.g., `cli: add refresh-exchange pipeline command`, `eodhd: infer EPS unit...`). Keep subjects short and omit trailing periods.
- PRs should include a clear description, relevant command output (e.g., `pytest`), and documentation updates when CLI behavior or screens change.
- If you touch schema or persistence, call out changes in `src/pyvalue/migrations.py` and any impact on `data/pyvalue.db`.

## Configuration & Data Notes
- Store credentials in `private/config.toml` or environment variables like `PYVALUE_SEC_USER_AGENT`; never commit secrets.
- Screen outputs default to `data/screen_results_*.csv` and are treated as local artifacts.
- If you import a new third-party package, add it to `pyproject.toml` under `[project].dependencies`.
- Always check whether `README.md` needs updates when behavior, metrics, or CLI usage changes.
