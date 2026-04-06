# Repository Guidelines

## Project Description
pyvalue is a Python toolkit for ingesting, normalizing, and screening fundamental market data with a focus on value-investing workflows. It supports SEC (US-only) and EODHD data sources, stores results in SQLite, and exposes a CLI for universe loading, metric computation, and screening.

## Agent Operating Rules
- Enter plan mode for any non-trivial task.
- Use plan mode for verification and review work, not only for building.
- Write a detailed spec up front for non-trivial work to reduce ambiguity before implementation.
- Use subagents liberally to keep the main context window clean.
- Offload research, exploration, and parallel analysis to subagents.
- Give each subagent exactly one focused task.
- Don't create feature branches. Always work on `main` unless the user explicitly instructs otherwise.
- Keep `CLAUDE.md` as an exact copy of `AGENTS.md`. Whenever you update `AGENTS.md`, update `CLAUDE.md` in the same change.
- When you notice a repeatable mistake pattern, write a preventive rule for yourself.
- Never mark a task complete without proving it works.
- For non-trivial changes, pause and ask whether there is a more elegant solution.
- If the current fix looks hacky, replace it with the elegant solution you would choose knowing everything you know now.
- Skip the elegance pass for simple fixes; do not over-engineer.
- Challenge your own work before presenting it.
- After any correction from the user, update `tasks/lessons.md` with the pattern.
- Ruthlessly iterate on `tasks/lessons.md` until the same mistake stops recurring.

## Pyvalue-Specific Agent Review Rules
- When querying the database, check the sign of field values and verify the metric formula still makes sense with those signs. If you find a sign-related bug, fix it and report it.
- Keep track of why a metric cannot be calculated for a stock or exchange: missing data, a calculation bug, insufficient history, or an overly strict horizon such as 10Y versus 5Y.
- If a screen returns very few hits, identify which criterion eliminates the most stocks and consider whether the threshold is too strict or whether a different metric would be better.
- When reviewing metric calculations, verify that all input fields use compatible currencies. Apply the correct FX conversion when needed, and report issues you find.
- For UK stocks and UK exchanges, explicitly guard against mixing GBP and GBX.
- If a field value looks suspiciously small or large, flag it to the user.

## Database and SQL Design
When writing code involving databases, schema design, or SQL, treat performance as the highest priority unless the user explicitly says otherwise.

### Core Rules
- Start with access patterns: how the data will be filtered, joined, grouped, sorted, inserted, and updated.
- Design tables for the fastest expected production queries, not just for conceptual neatness.
- Choose data types carefully and keep rows as narrow as practical.
- Define primary keys, foreign keys, and uniqueness constraints deliberately.
- Propose and justify indexes based on actual query patterns.
- Avoid over-indexing, but do not leave important query paths unindexed.
- Be explicit about trade-offs between read speed, write speed, storage, and complexity.
- For large tables, consider partitioning, clustering, materialized summaries, or denormalization when they materially improve the expected workload.
- Avoid ORM-generated inefficiencies in performance-critical paths.

### Query Rules
- Write SQL for performance, not just correctness.
- Avoid `SELECT *` in production code.
- Minimize full table scans, unnecessary sorts, repeated subqueries, and N+1 query patterns.
- Use joins, filters, aggregations, and pagination in ways that scale well.
- Check whether each important query can use an index efficiently.
- Call out queries that are likely to become slow at scale.

### Review Expectations
- If an existing schema, index strategy, or query design is inefficient, say so clearly.
- Report weak designs proactively, including missing or weak primary keys, wrong key choices, missing indexes, unused or redundant indexes, inefficient joins, wide or poorly typed columns, normalization or denormalization mistakes, and queries that do not match the schema design.
- Do not silently preserve a bad database design just because it already exists.
- When suggesting improvements, explain why they should be faster and what trade-offs they introduce.

### Output Expectations
When proposing database changes, include:
1. recommended schema design
2. key and index recommendations
3. expected query patterns
4. performance risks
5. better alternatives if the current design is weak

Performance-first database thinking is mandatory.

## Project Structure & Module Organization
- `src/pyvalue/`: core package. `ingestion/`, `normalization/`, `marketdata/`, `metrics/`, and `universe/` hold domain logic; `cli.py` wires the CLI; `storage.py` and `migrations.py` manage SQLite.
- `tests/`: pytest suite.
- `screeners/`: YAML screen definitions (e.g., `screeners/value.yml`).
- `data/`: local artifacts (`data/pyvalue.db`, logs, `data/screen_results_*.csv`).
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
- Whenever you make a change, explicitly decide whether documentation must be updated. Review the entire `docs/` tree to find the canonical file to update, and update `README.md` too when the repo landing-page guidance changes.
