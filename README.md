# pyvalue

Fundamental data ingestion and screening toolkit focusing on value-oriented strategies. Currently supports fetching the US equity universe.

## Quick start

```bash
python -m pip install -e .[dev]
pytest
```

## US universe loader

```python
from pyvalue.universe import USUniverseLoader

loader = USUniverseLoader()
universe = loader.load()
for item in universe:
    print(item.symbol, item.exchange)
```

The loader downloads Nasdaq Trader symbol directories, filters out test issues, and normalizes exchange names across NASDAQ, NYSE, NYSE Arca, NYSE MKT, and Cboe BZX.

## CLI persistence

Persist the US universe into a local SQLite database via the CLI:

```bash
pyvalue load-us-universe --database data/pyvalue.db
```

ETFs are excluded by default; pass `--include-etfs` to store them as well.

> Nasdaq serves the symbol directories via FTP (`ftp://ftp.nasdaqtrader.com/symboldirectory/...`).
> You can verify availability manually with
> `curl "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt"`.

## Private configuration

Place API keys or region-specific credentials inside the `private/` directory (ignored by git).
