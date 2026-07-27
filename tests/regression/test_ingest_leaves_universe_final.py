"""Regression: ingesting fundamentals must leave the universe settled.

Author: Emre Tezel

``listing.primary_listing_status`` and issuer identity are both derived, and
both used to be maintained by separate reconcile commands. A bootstrap therefore
ended in a state that was correct only after two extra passes, and any later
ingest drifted until someone remembered to re-run them.

``ingest-fundamentals`` now settles both as it stores each batch. The property
that makes that safe is the one these tests exist to hold:

    **ingestion order must not affect the final state, and running either
    reconcile command afterwards must change nothing.**

It is not free. Neither the inherit-from-an-ISIN-peer rule nor the sole-listing
rescue can be decided from one payload, and issuer grouping needs every member of
a group at once, so ingest re-evaluates each batch's whole neighbourhood rather
than only the listings that arrived. ``0K10.LSE`` carries no ``PrimaryTicker``
and is decided entirely by its peer ``MTD.US``; ``ARM.US`` is rescued only
because nothing else in the universe shares its identifiers. Without that
reach-back either answer would depend on which payload arrived first, and these
tests would fail.
"""

from __future__ import annotations

import random
import sqlite3
from pathlib import Path
from typing import Dict, FrozenSet, List, Set, Tuple

import pytest

from conftest import seed_exchange, seed_raw_fundamentals
from pyvalue.persistence.storage import (
    FundamentalsRepository,
    SupportedTickerRepository,
    UniverseReconciler,
)

# A fixture universe chosen to exercise every rule that can depend on order.
#
#   MTD/0K10   one security on two venues -- the peer rule
#   LULU/33L   one security, both self-declaring -- two primaries, by design
#              (the tie-break that used to demote 33L was removed in 2026-07);
#              still order-sensitive for issuer grouping, which must merge them
#   GOOG/GOOGL two securities, one entity -- the LEI bridge
#   DNLM/DNLMY an ADR that names itself primary -- HomeCategory must win
#   ARM        an exchange-listed ADR with no sibling -- the rescue
#   NETBAY     no identifiers at all -- stays unknown, stays eligible
_CATALOG: List[Tuple[str, str, str, str]] = [
    ("US", "MTD", "USD", "US5926881054"),
    ("LSE", "0K10", "USD", "US5926881054"),
    ("US", "LULU", "USD", "US5500211090"),
    ("F", "33L", "EUR", "US5500211090"),
    ("US", "GOOG", "USD", "US02079K1079"),
    ("US", "GOOGL", "USD", "US02079K3059"),
    ("LSE", "DNLM", "GBX", "GB00B1CKQ739"),
    ("US", "DNLMY", "USD", "US26543P1030"),
    ("US", "ARM", "USD", "US0420682058"),
    ("BK", "NETBAY", "THB", ""),
]

_ALPHABET_LEI = "5493006MHB84DD0ZWV18"

_PAYLOADS: Dict[str, dict] = {
    "MTD.US": {"Name": "Mettler-Toledo", "PrimaryTicker": "MTD.US", "Exchange": "NYSE"},
    # No PrimaryTicker at all -- faithful to the live payload.
    "0K10.LSE": {"Name": "Mettler-Toledo International Inc."},
    "LULU.US": {
        "Name": "lululemon athletica inc.",
        "PrimaryTicker": "LULU.US",
        "Exchange": "NASDAQ",
    },
    "33L.F": {"Name": "Lululemon Athletica Inc", "PrimaryTicker": "33L.F"},
    "GOOG.US": {
        "Name": "Alphabet Inc Class C",
        "PrimaryTicker": "GOOG.US",
        "Exchange": "NASDAQ",
        "LEI": _ALPHABET_LEI,
    },
    "GOOGL.US": {
        "Name": "Alphabet Inc Class A",
        "PrimaryTicker": "GOOGL.US",
        "Exchange": "NASDAQ",
        "LEI": _ALPHABET_LEI,
    },
    "DNLM.LSE": {
        "Name": "Dunelm Group PLC",
        "PrimaryTicker": "DNLM.LSE",
        "LEI": "213800WCOWEI3T5DUV19",
    },
    "DNLMY.US": {
        "Name": "Dunelm Group PLC ADR",
        "PrimaryTicker": "DNLMY.US",
        "HomeCategory": "ADR",
        "Exchange": "PINK",
    },
    "ARM.US": {
        "Name": "Arm Holdings plc",
        "PrimaryTicker": "ARM.US",
        "HomeCategory": "ADR",
        "Exchange": "NASDAQ",
    },
    "NETBAY.BK": {"Name": "Netbay Public Company Limited"},
}


def _bootstrap_catalog(db_path: Path) -> None:
    """Catalog every listing, exactly as ``refresh-supported-tickers`` would.

    Always in the same order, so issuer ids are assigned identically across
    runs; only the *ingest* order varies between the cases below.
    """

    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    by_exchange: Dict[str, List[dict]] = {}
    for exchange, code, currency, isin in _CATALOG:
        row: dict = {
            "Code": code,
            "Name": code,
            "Type": "Common Stock",
            "Currency": currency,
        }
        if isin:
            row["Isin"] = isin
        by_exchange.setdefault(exchange, []).append(row)
    for exchange, rows in by_exchange.items():
        seed_exchange(db_path, exchange)
        repo.replace_for_exchange("EODHD", exchange, rows)
    FundamentalsRepository(db_path).initialize_schema()


def _ingest(db_path: Path, order: List[str]) -> None:
    """Store payloads one at a time, in ``order``.

    One payload per call means one batch per payload -- the strictest case, and
    the one where a missing reach-back shows up.
    """

    for symbol in order:
        seed_raw_fundamentals(db_path, "EODHD", symbol, {"General": _PAYLOADS[symbol]})


def _snapshot(
    db_path: Path,
) -> Tuple[Dict[str, str], Set[Tuple[FrozenSet[str], str, str]]]:
    """Capture the derived state in an id-independent form.

    Issuer ids depend on how many rows a run had to allocate, so comparing them
    directly would fail for reasons that do not matter. What must match is the
    *partition* -- which symbols share a company -- and each group's LEI and
    name.
    """

    with sqlite3.connect(db_path) as conn:
        statuses = {
            str(symbol): str(status)
            for symbol, status in conn.execute(
                """
                SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
                FROM listing l
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                """
            )
        }
        grouped: Dict[int, Set[str]] = {}
        labels: Dict[int, Tuple[str, str]] = {}
        for issuer_id, symbol, lei, name in conn.execute(
            """
            SELECT l.issuer_id, l.symbol || '.' || e.exchange_code, i.lei, i.name
            FROM listing l
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            JOIN issuer i ON i.issuer_id = l.issuer_id
            """
        ):
            grouped.setdefault(int(issuer_id), set()).add(str(symbol))
            labels[int(issuer_id)] = (str(lei or ""), str(name))
    partition = {
        (frozenset(members), labels[issuer_id][0], labels[issuer_id][1])
        for issuer_id, members in grouped.items()
    }
    return statuses, partition


@pytest.mark.parametrize("seed", [0, 1, 2, 7, 13])
def test_ingest_order_does_not_change_the_final_state(
    tmp_path: Path, seed: int
) -> None:
    """Any ingestion order must produce the same universe.

    A single fixed order would pass while hiding exactly the bug this design
    exists to prevent, so the order is shuffled per seed and compared against
    the catalog order.
    """

    reference = tmp_path / f"reference-{seed}.db"
    _bootstrap_catalog(reference)
    _ingest(reference, list(_PAYLOADS))
    expected = _snapshot(reference)

    shuffled_order = list(_PAYLOADS)
    random.Random(seed).shuffle(shuffled_order)

    shuffled = tmp_path / f"shuffled-{seed}.db"
    _bootstrap_catalog(shuffled)
    _ingest(shuffled, shuffled_order)

    assert _snapshot(shuffled) == expected, f"order {shuffled_order} diverged"


@pytest.mark.parametrize("seed", [0, 3, 11])
def test_reconcile_after_ingest_changes_nothing(tmp_path: Path, seed: int) -> None:
    """The whole point: a reconcile is a no-op on a freshly ingested catalog.

    If this fails, ingest did not leave the universe final and the reconcile
    commands are still load-bearing rather than repair tools.
    """

    order = list(_PAYLOADS)
    random.Random(seed).shuffle(order)

    db_path = tmp_path / f"final-{seed}.db"
    _bootstrap_catalog(db_path)
    _ingest(db_path, order)
    before = _snapshot(db_path)

    result = UniverseReconciler(db_path).reconcile()

    assert _snapshot(db_path) == before
    assert result.groups_merged == 0
    assert result.listings_repointed == 0
    assert result.issuers_created == 0
    assert result.issuers_deleted == 0
    assert result.leis_assigned == 0


def test_the_universe_ingest_produces_is_correct(tmp_path: Path) -> None:
    """Pin the actual verdicts, not just their stability across orders.

    Order-independence alone would be satisfied by being consistently wrong.
    """

    db_path = tmp_path / "verdicts.db"
    _bootstrap_catalog(db_path)
    _ingest(db_path, list(_PAYLOADS))
    statuses, partition = _snapshot(db_path)

    assert statuses == {
        "MTD.US": "primary",
        # No PrimaryTicker of its own; inherits its ISIN peer's answer.
        "0K10.LSE": "secondary",
        # Both self-declare on one ISIN and nothing EODHD publishes says which
        # is wrong, so both stand. The tie-break that used to demote the
        # Frankfurt line ranked venues from a hand-coded map; it was removed in
        # 2026-07. They are still merged onto one issuer below -- identity is
        # settled by the shared ISIN, which needs no venue knowledge.
        "LULU.US": "primary",
        "33L.F": "primary",
        "GOOG.US": "primary",
        "GOOGL.US": "primary",
        "DNLM.LSE": "primary",
        # Names itself primary, but HomeCategory says ADR and it trades OTC.
        "DNLMY.US": "secondary",
        # Also labelled ADR, but on NASDAQ with no surviving sibling -- rescued
        # to unknown rather than erased, and unknown stays eligible.
        "ARM.US": "unknown",
        # No identifiers at all.
        "NETBAY.BK": "unknown",
    }

    groups = {members for members, _lei, _name in partition}
    # One security on two venues is one company.
    assert frozenset({"MTD.US", "0K10.LSE"}) in groups
    assert frozenset({"LULU.US", "33L.F"}) in groups
    # Two securities, one entity -- only the shared LEI links them.
    assert frozenset({"GOOG.US", "GOOGL.US"}) in groups
    # A receipt publishing no LEI cannot be linked to its underlying.
    assert frozenset({"DNLM.LSE"}) in groups
    assert frozenset({"DNLMY.US"}) in groups

    alphabet = next(lei for members, lei, _ in partition if "GOOG.US" in members)
    assert alphabet == _ALPHABET_LEI


def test_reingesting_unchanged_payloads_changes_nothing(tmp_path: Path) -> None:
    """A steady-state re-ingest must not churn the derived rows.

    Every write is guarded, so the statements still run but change no rows --
    which is why this counts *rows changed* rather than statements issued.
    Without the guards a routine refresh would rewrite the whole universe:
    ~76k listing rows per pass, for nothing.
    """

    db_path = tmp_path / "reingest.db"
    _bootstrap_catalog(db_path)
    _ingest(db_path, list(_PAYLOADS))
    before = _snapshot(db_path)

    _ingest(db_path, list(_PAYLOADS))
    settled = UniverseReconciler(db_path).reconcile()

    assert _snapshot(db_path) == before
    assert settled.statuses_changed == 0
    assert settled.listings_repointed == 0
    assert settled.issuers_created == 0
    assert settled.issuers_deleted == 0
    assert settled.leis_assigned == 0
