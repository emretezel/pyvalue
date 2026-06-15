"""Test configuration helpers.

Author: Emre Tezel
"""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional

# Ensure the src/ directory is importable when running tests without installation.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

if TYPE_CHECKING:
    # Type-only imports for the seed-helper signatures. ``from __future__ import
    # annotations`` makes every annotation a lazy string, and the runtime bodies
    # import their repositories lazily (after the src/ path insert above), so
    # these never execute at import time -- they exist purely so mypy can resolve
    # the annotations.
    from pyvalue.currency import MetricUnitKind
    from pyvalue.persistence.storage import FactRecord, MetricComputeStatusRecord


def seed_exchange(
    db_path: Path,
    *codes: str,
    provider: str = "EODHD",
    currency: str = "USD",
) -> None:
    """Seed one or more existing provider_exchange rows for tests.

    The exchange catalog (``exchange`` + ``provider_exchange``) is owned by
    ``refresh-supported-exchanges``. ``replace_for_exchange`` /
    ``replace_from_listings`` now resolve the provider_exchange read-only and
    raise if it is absent, so any test that catalogs listings must seed the
    exchange first. Each upsert is non-destructive (it never prunes siblings),
    so a test can seed every exchange code it uses in a single call. Defaults to
    ``"US"`` when no code is given; the canonical exchange code mirrors the
    provider exchange code.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import ExchangeProviderRepository

    repo = ExchangeProviderRepository(db_path)
    for code in codes or ("US",):
        repo.ensure_fixed_exchange(provider, code, code, currency=currency)


def _resolve_seeded_listing_id(db_path: Path, symbol: str) -> int:
    """Resolve a canonical ``symbol`` to its listing id for the id-keyed seeders.

    Production storage exposes only id-keyed writers, so these test seeders must
    resolve the symbol to its ``listing_id`` first. The listing must already be
    catalogued (e.g. via ``seed_exchange`` + a supported-ticker write); a missing
    listing is a test-setup bug, surfaced here as an ``AssertionError`` rather
    than silently minting a row.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import SecurityRepository

    listing_id = SecurityRepository(db_path).resolve_id(symbol)
    assert listing_id is not None, (
        f"listing {symbol!r} must be seeded before id-keyed test writes "
        "(seed the catalog first, e.g. via seed_exchange + a supported-ticker write)"
    )
    return listing_id


def seed_metric(
    db_path: Path,
    symbol: str,
    metric_id: str,
    value: float,
    as_of: str,
    unit_kind: MetricUnitKind = "other",
    currency: Optional[str] = None,
    unit_label: Optional[str] = None,
) -> None:
    """Seed one stored metric row for an already-catalogued ``symbol``.

    Resolves ``symbol`` to its ``listing_id`` and persists through the id-keyed
    :meth:`MetricsRepository.upsert_many_by_id` (the only metric writer).
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import MetricsRepository

    listing_id = _resolve_seeded_listing_id(db_path, symbol)
    MetricsRepository(db_path).upsert_many_by_id(
        [(listing_id, metric_id, value, as_of, unit_kind, currency, unit_label)]
    )


def seed_metric_status(
    db_path: Path,
    *records: MetricComputeStatusRecord,
) -> None:
    """Seed metric-compute-status rows for already-catalogued symbols.

    Each ``record`` carries a display ``symbol``; this resolves it to a
    ``listing_id`` and re-keys the record for the id-keyed
    :meth:`MetricComputeStatusRepository.upsert_many_by_id` (the only status
    writer), dropping the now-redundant ``symbol``.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import MetricComputeStatusRepository

    id_records = []
    for record in records:
        assert record.symbol is not None, "seed_metric_status requires record.symbol"
        listing_id = _resolve_seeded_listing_id(db_path, record.symbol)
        id_records.append(replace(record, listing_id=listing_id, symbol=None))
    MetricComputeStatusRepository(db_path).upsert_many_by_id(id_records)


def seed_price(
    db_path: Path,
    symbol: str,
    as_of: str,
    price: float,
    volume: Optional[int] = None,
    currency: Optional[str] = None,
) -> None:
    """Seed one market-data row for an already-catalogued ``symbol``.

    Resolves ``symbol`` to its ``listing_id`` and persists through the id-keyed
    :meth:`MarketDataRepository.upsert_prices` (the only price writer).
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.marketdata import MarketDataUpdate
    from pyvalue.persistence.storage import MarketDataRepository

    listing_id = _resolve_seeded_listing_id(db_path, symbol)
    MarketDataRepository(db_path).upsert_prices(
        [
            MarketDataUpdate(
                security_id=listing_id,
                symbol=symbol,
                as_of=as_of,
                price=price,
                volume=volume,
                currency=currency,
            )
        ]
    )


def seed_facts(
    db_path: Path,
    symbol: str,
    records: Iterable[FactRecord],
) -> int:
    """Replace the stored facts for an already-catalogued ``symbol``.

    Resolves ``symbol`` to its ``listing_id``, projects each :class:`FactRecord`
    to the stored-row tuple shape, and persists through the id-keyed
    :meth:`FinancialFactsRepository.replace_fact_rows` (the only fact writer).
    Returns the number of rows written.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import FinancialFactsRepository

    listing_id = _resolve_seeded_listing_id(db_path, symbol)
    return FinancialFactsRepository(db_path).replace_fact_rows(
        listing_id,
        [
            (
                record.concept,
                record.fiscal_period,
                record.end_date,
                record.unit_kind,
                record.value,
                record.filed,
                record.currency,
            )
            for record in records
        ],
    )


def seed_security_metadata(
    db_path: Path,
    symbol: str,
    entity_name: Optional[str] = None,
    description: Optional[str] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
) -> None:
    """Seed canonical metadata for an already-catalogued ``symbol``.

    Resolves ``symbol`` to its ``listing_id`` and persists through the id-keyed
    :meth:`SecurityRepository.upsert_metadata_many` (the only metadata writer).
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import SecurityMetadataUpdate, SecurityRepository

    listing_id = _resolve_seeded_listing_id(db_path, symbol)
    SecurityRepository(db_path).upsert_metadata_many(
        [
            SecurityMetadataUpdate(
                security_id=listing_id,
                entity_name=entity_name,
                description=description,
                sector=sector,
                industry=industry,
            )
        ]
    )
