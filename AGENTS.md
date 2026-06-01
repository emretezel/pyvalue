# Repository Guidelines

## Project Description
pyvalue is a Python toolkit for ingesting, normalizing, and screening fundamental market data with a focus on value-investing workflows. It uses EODHD as its data source, stores results in SQLite, and exposes a CLI for universe loading, metric computation, and screening.

## Agent Operating Rules

### Workflow
- Enter plan mode for any non-trivial task. Use plan mode for verification and review work, not only for building.
- Don't create feature branches. Always work on `main` unless the user explicitly instructs otherwise.
- Keep `CLAUDE.md` and `AGENTS.md` byte-identical. Whenever you update one, update the other in the same change.

### Design & Elegance
- **Pick the right pattern up front.** Before implementing, identify the most appropriate design pattern and apply sound object-oriented (or functional, where idiomatic) principles.
- **Simplest correct solution wins.** Prefer the simplest solution that is correct and maintainable. Do not over-engineer straightforward tasks; skip the elegance pass for simple fixes.
- **Refactoring is part of every feature, not optional.** When adding or changing code, check whether the surrounding design is still the most elegant solution. If not, refactor it before moving on. If the current fix looks hacky, replace it with the elegant solution you would choose knowing everything you know now.
- **File length.** If a Python module grows too long and is becoming hard to maintain, refactor it into multiple modules along its natural responsibility seams. There is no fixed line-count rule — use judgement based on how many distinct responsibilities the file has accumulated and whether readers can still navigate it comfortably.
- **Ongoing design review.** As the project grows, whenever you are working on a part of the code, review whether that part should be refactored to better adhere to established design patterns and object-oriented principles. Long-term maintainability, ease of change, readability, and the ability to add new features without friction are paramount. Do not defer this review — if a structural improvement is warranted, propose it to the author before moving on.
- **Revisit tool and library choices.** As the project grows, periodically reassess the libraries, frameworks, and tools in use. If a better-fit alternative exists, propose the migration to the author with a concrete rationale (what it improves, what it costs, what it breaks) before switching.

## Pyvalue-Specific Agent Review Rules
- When querying the database, check the sign of field values and verify the metric formula still makes sense with those signs. If you find a sign-related bug, fix it and report it.
- Keep track of why a metric cannot be calculated for a stock or exchange: missing data, a calculation bug, insufficient history, or an overly strict horizon such as 10Y versus 5Y.
- If a screen returns very few hits, identify which criterion eliminates the most stocks and consider whether the threshold is too strict or whether a different metric would be better.
- When reviewing metric calculations, verify that every monetary input is converted to the listing's currency before it is combined with another. Metrics convert each input to the listing currency via the `fx_rates` table through the single `require_metric_money` seam (`metrics/utils.py`), logging every conversion; a missing FX rate makes the metric unavailable (a structured `missing_fx_rate` reason) rather than silently mixing currencies. Subunits (GBX/ZAC/ILA) are collapsed to their major unit at the data boundary and never reach metric arithmetic.
- For UK stocks and UK exchanges, explicitly guard against mixing GBP and GBX.
- If a field value looks suspiciously small or large, flag it to the user.

## Database and SQL Design

### Schema Design Principles
- **Single source of truth.** Every fact lives in exactly one place. Never replicate a value across tables to avoid a join.
- **One table, one thing.** A table models exactly one entity, event, or relationship. Mixing concerns is a design smell.
- **Normalise to at least 3NF by default.** Deviate only when a clear, justified performance need exists — and document the deviation explicitly.
- **Primary keys: meaningful and minimal.** Prefer natural keys when they are genuinely stable and unique; use surrogate keys only when no natural key exists or the natural key is composite and unwieldy.
- **Always declare foreign keys.** Referential integrity is enforced at the schema level, not in application code. Ensure SQLite is opened with `PRAGMA foreign_keys = ON` so the constraints are actually enforced.
- **Always declare UNIQUE constraints** on every column or combination that is semantically unique, regardless of whether it is also the primary key.
- **Default to NOT NULL.** A column is nullable only when the absence of a value is a meaningful, valid state.
- **Use the most precise data type that correctly represents the domain** (`DATE` / ISO-8601 `TEXT` for dates, integer minor units or `NUMERIC` affinity for money and prices — never `REAL`/floating point for monetary or price values, since floating-point error is unacceptable in a value-investing/trading context).
- **No magic values.** Never use sentinel values (`0`, `-1`, `"N/A"`) to represent absence or special states — use `NULL` or a proper status column with a `CHECK` constraint.
- **CHECK constraints encode invariants.** Domain rules (e.g. `quantity > 0`, `side IN ('BUY','SELL')`, valid enum values, currency code length) must be `CHECK` constraints so the database enforces them.

### Indexes
- Add indexes after the schema is correct. Never let a performance desire drive a denormalisation decision.
- Justify each index: name the query pattern it serves.
- Avoid redundant indexes (e.g. an index whose leading columns are already covered by another).
- Use views to pre-compose common joins or projections without duplicating data.

### Schema Evolution
Whenever features are added or code is refactored, re-evaluate the schema. If the design can be improved, plan and apply the necessary migrations — do not silently preserve a bad design because it already exists. Migrations for this project live in `src/pyvalue/persistence/migrations.py`; add new migrations there in order, and call out any impact on `data/pyvalue.db`.

### SQL Style
- Write correct SQL first; optimise second.
- Never use `SELECT *` in production code — name every column explicitly.
- Do not repeat logic that belongs in the schema (e.g. filtering soft-deleted rows in every query instead of defining a view).
- Check whether each important query can use an index efficiently; use `EXPLAIN QUERY PLAN` for non-trivial queries.

### Review Expectations
Flag — and propose corrections for — any schema that:
- Duplicates a fact or violates normal form without justification
- Uses an imprecise data type (especially `REAL`/floating point for money or prices)
- Omits a constraint that should exist (NOT NULL, UNIQUE, FOREIGN KEY, CHECK)
- Conflates multiple entities in one table
- Uses magic values instead of proper nullability or `CHECK` constraints

When proposing schema changes, always include:
1. Recommended schema with all constraints stated explicitly
2. Normalisation rationale (target normal form and why)
3. Index recommendations with the query patterns they serve
4. Justification for any deliberate deviation from normal form

## Project Structure & Module Organization

### Layout Principles
- Choose the folder layout that best matches the project type and language conventions. For Python, default to a `src/`-layout package with `tests/` alongside — pyvalue follows this convention.
- Re-evaluate the structure after significant changes. If a better layout has become clear, reorganise — a clean structure is worth the churn.

### Current Layout
- `src/pyvalue/`: core package. `ingestion/`, `normalization/`, `marketdata/`, `metrics/`, `money/`, `screening/`, and `universe/` hold domain logic; the `cli/` package wires the CLI; the `persistence/` package (`storage/`, `migrations.py`, `database_review_docs.py`) manages SQLite. Foundational modules (`currency.py`, `facts.py`, `config.py`, `logging_utils.py`, `reporting.py`) stay at the package root.
- `tests/`: pytest suite, mirroring the source tree. Use `tests/unit/`, `tests/regression/`, and `tests/integration/` subdirectories; the canonical layout is documented in `docs/architecture.md`.
- `screeners/`: YAML screen definitions (e.g., `screeners/value.yml`).
- `data/`: local artifacts (`data/pyvalue.db`, logs, `data/screen_results_*.csv`).
- `private/`: local config and API keys (`private/config.toml`, gitignored).
- `docs/`: substantive documentation (see the **Documentation** section).

## Build, Test, and Development Commands
- `python -m pip install -e .[dev]`: install editable package (Python >=3.9).
- Use the `pyvalue` conda environment for all project commands.
- `conda run -n pyvalue pytest`: run the test suite in the `pyvalue` conda env.
- `pyvalue --help`: list CLI entry points; examples in `README.md` show workflows.
- **Packaging is a first-class concern.** `pyproject.toml` must always be sufficient on its own to: (a) build a distributable package, (b) carry the project version, (c) install pyvalue for end users (`pip install .`), and (d) install it in development mode for contributors (`pip install -e .[dev]`). Do not introduce setup steps that live outside the project metadata file.

## Coding Style & Naming Conventions

### Naming
- Use 4-space indentation and standard Python conventions: `snake_case` for modules/functions, `CapWords` for classes, and `UPPER_SNAKE_CASE` for constants.
- Follow the patterns established in `src/pyvalue/cli.py`.

### Comments & Documentation in Code
- **Heavily commented — explain the why, not the what.** New code must include comments around non-obvious logic, business rules, sign/unit conventions, currency invariants, and design decisions. Aim at the *motivation*; the line-by-line behaviour should already be clear from the code.
- **Module headers.** Every new module/file must open with a triple-quoted docstring (`""" ... """`) that briefly describes the module's purpose and names the author.
- **Type annotations everywhere.** Every function signature, method, attribute, and module-level constant carries type hints. There is no "where they already exist" loophole — full coverage is the standard, and `mypy src/ tests/` enforces it.
- **Public docstrings.** Every public function, method, and class must have a docstring covering its purpose, inputs, outputs, and any non-obvious behaviour.

### Formatting
- Format with `ruff format`; import order is enforced by `ruff` — do not reorder manually.
- Keep diffs consistent with surrounding code.

## Testing Guidelines

### General
- **No change lands without tests.** New functionality → new unit tests. Changed behaviour → updated tests. Bug fix → a regression test that *fails on the buggy code and passes on the fix*.
- The test suite covers both **unit tests** (pure functions, isolated logic) and **regression tests** (one per fixed bug).
- Run the full suite before every commit: `conda run -n pyvalue pytest`. A green suite is a prerequisite for pushing, opening a PR, or declaring a task done.
- The canonical test directory layout is documented in `docs/architecture.md`. If the layout needs to change, confirm with the author and update `docs/architecture.md` in the same commit.

### Test Structure (Python / pytest)
- Framework: `pytest` (configured in `pyproject.toml`).
- Mirror the source tree under `tests/` with `tests/unit/`, `tests/regression/`, and `tests/integration/` subdirectories. File naming `test_*.py`, function naming `test_*`.
- Tag slow, integration, and regression tests with `pytest.mark` so they can be run selectively during development and run fully in CI.
- Prefer fixtures in `tests/conftest.py` over setup/teardown boilerplate — keep tests readable.
- Measure coverage with `pytest-cov`; aim for high coverage on business logic (metrics, normalisation, screening, currency handling).

### Mandatory Test Categories
Pyvalue computes numerical metrics over a screened universe; the same disciplines a trading system needs apply here:
- **Unit tests for every pure function** — metric calculations, normalisation routines, currency/FX/sign/unit guards, screen evaluators.
- **Property-based tests** (e.g. via [Hypothesis](https://hypothesis.readthedocs.io/)) for numerical and invariant-bearing code: metric formulas, FX/currency invariants, normalisation idempotence, monotonicity properties of screens.
- **Reproducibility tests** — a fixed input universe plus fixed metric inputs must produce a bit-identical screen output (CSV byte-equal) across runs.
- **Integration tests against a small, checked-in fixture dataset** — exercise the pipeline end-to-end (ingest → normalise → metrics → screen) without hitting external APIs.

## Quality Gate

Every commit must pass all four quality-gate tools with **zero errors** before being pushed, opening a PR, or marking work done. Run them in the `pyvalue` conda env:

| Tool | Command |
|---|---|
| Format | `conda run -n pyvalue ruff format .` |
| Lint | `conda run -n pyvalue ruff check .` |
| Type check | `conda run -n pyvalue mypy src/ tests/` |
| Tests | `conda run -n pyvalue pytest` |

### Universal rule
The whole codebase must be clean — "the file I touched is clean" is not enough. Pre-existing errors are not an excuse to introduce new ones; if you find them, fix them.

### mypy (non-negotiable)
- `mypy src/ tests/` must report zero errors at the end of every change.
- Fix mypy errors by correcting the design — **no `# type: ignore`, no `Any` / `object` widening to silence the checker, no removing or loosening annotations**. If mypy is unhappy, the type model is telling you something — fix it.

### ruff
- All code must pass `ruff format` and `ruff check` with no suppressions, except where a suppression has an explicit, documented reason in the surrounding comment.
- Import order is enforced by ruff — do not reorder manually.

## Commit & Pull Request Guidelines
- Commit messages in history are concise, imperative, and often scoped (e.g., `cli: add refresh-exchange pipeline command`, `eodhd: infer EPS unit...`). Keep subjects short and omit trailing periods.
- PRs should include a clear description and relevant command output (e.g., `pytest`).
- If you touch schema or persistence, call out changes in `src/pyvalue/persistence/migrations.py` and any impact on `data/pyvalue.db`.

## Documentation
- **`README.md` stays short.** The repo-root `README.md` covers project purpose, installation instructions, and the two or three most common usage examples — nothing else. Promote longer material to `docs/` rather than letting `README.md` grow. Update `README.md` when the project's purpose, install steps, or headline examples change.
- **Substantive documentation lives under `docs/`.** Keep the tree organised; create a subdirectory whenever a topic area grows beyond two or three files.
- **Database documentation: `docs/database/`.** One Markdown file per table, showing all fields, types, constraints (PK, FK, UNIQUE, CHECK, NOT NULL), indexes, and any views that reference the table.
- **Keep documentation in sync with the code.** When you change behaviour, update the relevant doc file in the same commit (and the per-table file under `docs/database/` for any schema change). For each change, explicitly decide whether docs need an update and locate the canonical file in `docs/` to edit.

## Configuration & Data Notes
- Store credentials in `private/config.toml` (for example `[eodhd].api_key`); never commit secrets.
- Screen outputs default to `data/screen_results_*.csv` and are treated as local artifacts.
- If you import a new third-party package, add it to `pyproject.toml` under `[project].dependencies`.
