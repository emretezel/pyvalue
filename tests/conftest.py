"""Test configuration helpers.

Author: Emre Tezel
"""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Sequence

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
    from pyvalue.persistence.storage import (
        FactRecord,
        MetricComputeStatusRecord,
        SupportedTickerRefreshResult,
    )
    from pyvalue.universe import Listing


def seed_exchange(
    db_path: Path,
    *codes: str,
    provider: str = "EODHD",
    currency: str = "USD",
) -> None:
    """Seed one or more existing provider_exchange rows for tests.

    The exchange catalog (``exchange`` + ``provider_exchange``) is owned by
    ``refresh-supported-exchanges``. ``replace_for_exchange`` resolves the
    provider_exchange read-only and raises if it is absent, so any test that
    catalogs listings must seed the exchange first. Each upsert is
    non-destructive (it never prunes siblings),
    so a test can seed every exchange code it uses in a single call. Defaults to
    ``"US"`` when no code is given; the canonical exchange code mirrors the
    provider exchange code.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import ExchangeProviderRepository

    repo = ExchangeProviderRepository(db_path)
    for code in codes or ("US",):
        repo.ensure_fixed_exchange(provider, code, code, currency=currency)


def seed_supported_listings(
    db_path: Path,
    provider: str,
    exchange_code: str,
    listings: Sequence[Listing],
) -> SupportedTickerRefreshResult:
    """Catalog supported listings for tests via production ``replace_for_exchange``.

    Mirrors the (now-deleted) ``replace_from_listings`` test seam: tests build
    ``Listing`` DTOs; this converts each to the EODHD provider-row shape the
    production writer consumes (a bare ``Code`` with the exchange suffix stripped,
    plus ``Name``/``Currency``) and replaces the whole (provider, exchange) slice.
    The provider_exchange must already be seeded (e.g. via :func:`seed_exchange`).
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import SupportedTickerRepository

    rows = [
        {
            "Code": listing.symbol.strip().upper().split(".")[0],
            "Name": listing.security_name,
            "Currency": listing.currency,
        }
        for listing in listings
    ]
    return SupportedTickerRepository(db_path).replace_for_exchange(
        provider, exchange_code, rows
    )


def resolve_listing_id(db_path: Path, symbol: str) -> int:
    """Resolve a canonical ``symbol`` to its listing id for id-keyed tests.

    Production storage exposes only id-keyed writers and a single batch
    symbol->id resolver (``resolve_ids_many``), so these tests resolve the symbol
    to its ``listing_id`` through that same resolver. The listing must already be
    catalogued (e.g. via ``seed_exchange`` + a supported-ticker write); a missing
    listing is a test-setup bug, surfaced here as an ``AssertionError`` rather
    than silently minting a row.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import SecurityRepository

    resolved = SecurityRepository(db_path).resolve_ids_many([symbol])
    listing_id = next(iter(resolved.values()), None)
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

    listing_id = resolve_listing_id(db_path, symbol)
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
        listing_id = resolve_listing_id(db_path, record.symbol)
        id_records.append(replace(record, listing_id=listing_id, symbol=None))
    MetricComputeStatusRepository(db_path).upsert_many_by_id(id_records)


def resolve_provider_listing_id(db_path: Path, listing_id: int) -> Optional[int]:
    """Resolve a ``listing_id`` to its provider mapping, ``None`` when unmapped.

    Lenient by design: a canonical listing legitimately outlives its
    ``provider_listing`` row (the delisting purge removes only the provider
    layer), so tests seeding data for such listings get ``None`` rather than
    an assertion -- matching the production canonical-only write path.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT provider_listing_id FROM provider_listing WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()
    return int(row[0]) if row is not None else None


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
    :meth:`MarketDataRepository.upsert_prices` (the only price writer). The
    provider mapping is threaded too, so a provider-catalogued listing seeds
    both layers (``provider_market_data`` + canonical ``market_data``) exactly
    like the production refresh; a listing without a ``provider_listing`` row
    seeds canonical-only.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.marketdata import MarketDataUpdate
    from pyvalue.persistence.storage import MarketDataRepository

    listing_id = resolve_listing_id(db_path, symbol)
    provider_listing_id = resolve_provider_listing_id(db_path, listing_id)
    MarketDataRepository(db_path).upsert_prices(
        [
            MarketDataUpdate(
                security_id=listing_id,
                symbol=symbol,
                as_of=as_of,
                price=price,
                volume=volume,
                currency=currency,
                provider_listing_id=provider_listing_id,
            )
        ]
    )


def fundamentals_payload_exists(db_path: Path, provider: str, symbol: str) -> bool:
    """True if a raw fundamentals payload is stored for ``symbol``.

    A test-side existence probe: it resolves the symbol through the
    ``provider_listing_catalog`` view (provider_symbol -> provider_listing_id) and
    checks ``fundamentals_raw`` (PK'd on provider_listing_id). Production reads
    payloads only by id; tests assert by symbol. The provider_listing FK cascade
    means a removed listing leaves no orphaned payload, so a missing catalog row
    and a missing payload coincide -- matching the deleted symbol-keyed fetch.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM fundamentals_raw fr
            JOIN provider_listing_catalog c
              ON c.provider_listing_id = fr.provider_listing_id
            WHERE c.provider = ? AND c.provider_symbol = ?
            """,
            (provider.strip().upper(), symbol.strip().upper()),
        ).fetchone()
    return row is not None


def normalization_state_exists(db_path: Path, provider: str, symbol: str) -> bool:
    """True if a fundamentals normalization-state row exists for ``symbol``.

    The norm-state counterpart of :func:`fundamentals_payload_exists`; see its
    docstring for why this test-side probe resolves by symbol through the catalog
    view while production keys ``fundamentals_normalization_state`` by id.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM fundamentals_normalization_state s
            JOIN provider_listing_catalog c
              ON c.provider_listing_id = s.provider_listing_id
            WHERE c.provider = ? AND c.provider_symbol = ?
            """,
            (provider.strip().upper(), symbol.strip().upper()),
        ).fetchone()
    return row is not None


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

    listing_id = resolve_listing_id(db_path, symbol)
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

    listing_id = resolve_listing_id(db_path, symbol)
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


def _resolve_seeded_provider_listing_id(
    db_path: Path, provider: str, symbol: str
) -> int:
    """Resolve a catalogued ``(provider, symbol)`` to its ``provider_listing_id``.

    The provider-edge counterpart of :func:`resolve_listing_id`: the
    id-keyed normalization-state writer keys on ``provider_listing_id``, so this
    resolves it via the ``provider_listing`` natural key (provider code + bare
    provider symbol + provider exchange code). Splits on the *last* dot so multi-dot
    tickers (e.g. ``BRK.B.US``) resolve correctly. The listing must already be
    catalogued; a miss is a test-setup bug surfaced as an ``AssertionError``.
    """
    import sqlite3

    ticker, _, exchange = symbol.rpartition(".")
    bare = ticker or symbol
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT pl.provider_listing_id
            FROM provider_listing pl
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
            WHERE p.provider_code = ?
              AND pl.provider_symbol = ?
              AND px.provider_exchange_code = ?
            """,
            (
                provider.strip().upper(),
                bare.strip().upper(),
                (exchange or "US").strip().upper(),
            ),
        ).fetchone()
    assert row is not None, (
        f"provider listing {provider}:{symbol!r} must be catalogued before "
        "seeding normalization state (seed the catalog + a raw upsert first)"
    )
    return int(row[0])


def seed_normalization_success(
    db_path: Path,
    symbol: str,
    *,
    provider: str = "EODHD",
    payload_hash: str = "a" * 64,
) -> int:
    """Mark a catalogued listing's payload as normalized, keyed by id.

    Resolves ``(provider, symbol)`` to its ``provider_listing_id`` and writes the
    watermark through the id-keyed :meth:`mark_success_by_id` (the only
    normalization-state writer). ``payload_hash`` defaults to a valid 64-char
    placeholder for "just mark it normalized" seeds; pass a specific hash when the
    test asserts on the stored value. Returns the ``provider_listing_id``.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import FundamentalsNormalizationStateRepository

    provider_listing_id = _resolve_seeded_provider_listing_id(db_path, provider, symbol)
    FundamentalsNormalizationStateRepository(db_path).mark_success_by_id(
        provider_listing_id, payload_hash
    )
    return provider_listing_id


def seed_raw_fundamentals(
    db_path: Path,
    provider: str,
    symbol: str,
    payload: dict[str, object],
    exchange: Optional[str] = None,
) -> None:
    """Store a raw fundamentals payload for an already-catalogued ``symbol`` (id-keyed).

    Resolves the listing's ``provider_listing_id`` and ``listing_id`` and writes
    through the id-keyed :meth:`FundamentalsRepository.upsert_many` -- the production
    batch writer -- replacing the deleted single-payload ``upsert`` convenience. The
    ``provider_symbol`` carried on the update mirrors the qualified symbol the ingest
    path threads (the listing-status reconciliation reads it as data, not as a lookup).

    The argument order mirrors the old ``upsert(provider, symbol, payload, exchange)``
    with ``db_path`` prepended, so call sites convert by a prefix swap. ``exchange`` is
    accepted for that compatibility but ignored -- it is derived from ``symbol``.
    """
    del exchange
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import FundamentalsRepository, FundamentalsUpdate
    from pyvalue.persistence.storage.base import (
        _utc_now_iso,
        canonical_json_dumps,
        fundamentals_payload_hash,
    )

    provider_listing_id = _resolve_seeded_provider_listing_id(db_path, provider, symbol)
    listing_id = resolve_listing_id(db_path, symbol)
    data = canonical_json_dumps(payload)
    FundamentalsRepository(db_path).upsert_many(
        provider.strip().upper(),
        [
            FundamentalsUpdate(
                provider_listing_id=provider_listing_id,
                security_id=listing_id,
                provider_symbol=symbol.strip().upper(),
                data=data,
                payload_hash=fundamentals_payload_hash(data),
                last_fetched_at=_utc_now_iso(),
            )
        ],
    )
