# pyvalue

Tools for ingesting fundamental stock data from Financial Modeling Prep, storing it in SQLite via SQLAlchemy, and calculating ratios that support value-stock screening workflows.

## Installation

1. Create/activate a Python 3.9+ environment (e.g. `conda create -n pyvalue python=3.9` and `conda activate pyvalue`).
2. Install the package in editable mode:

   ```bash
   pip install -e .
   ```

3. (Optional) Install developer tooling, including `pytest`, with:

   ```bash
   pip install -e .[dev]
   ```

## Running tests

Execute the unit test suite with:

```bash
PYTHONPATH=src pytest
```

PyCharm users can also right-click `tests/test_data_models.py` and choose *Run* to execute the same suite inside the IDE.
