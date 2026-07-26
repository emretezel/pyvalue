"""Unit tests for neighbourhood resolution.

Author: Emre Tezel

The neighbourhood is what lets a narrow run -- one ingest batch, or
``--symbols AAPL.US`` -- reach the same answer a whole-universe pass would. If
it under-reaches, results become order-dependent; if it over-reaches, work is
wasted. Both directions are tested here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange
from pyvalue.persistence.storage import (
    FundamentalsRepository,
    SupportedTickerRepository,
)
from pyvalue.persistence.storage.universe_reconcile import (
    issuer_ids_for,
    resolve_neighbourhood,
)


def _catalog(db_path: Path, exchange: str, code: str, isin: str | None = None) -> None:
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, exchange)
    row: dict[str, object] = {
        "Code": code,
        "Name": f"{code} ({exchange})",
        "Type": "Common Stock",
        "Currency": "USD",
    }
    if isin is not None:
        row["Isin"] = isin
    repo.replace_for_exchange("EODHD", exchange, [row])


def _listing_id(db_path: Path, symbol: str, exchange: str) -> int:
    with sqlite3.connect(db_path) as conn:
        return int(
            conn.execute(
                """
                SELECT l.listing_id FROM listing l
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                WHERE l.symbol = ? AND e.exchange_code = ?
                """,
                (symbol, exchange),
            ).fetchone()[0]
        )


def _resolve(db_path: Path, seeds: list[int] | None) -> set[int]:
    with sqlite3.connect(db_path) as conn:
        return resolve_neighbourhood(conn, seeds)


def test_same_isin_listings_are_neighbours(tmp_path: Path) -> None:
    """A security's other venues must be visible, or the peer rules cannot fire."""

    db_path = tmp_path / "isin-link.db"
    isin = "US5926881054"
    _catalog(db_path, "US", "MTD", isin)
    _catalog(db_path, "LSE", "0K10", isin)
    _catalog(db_path, "AS", "ASML", "NL0010273215")

    mtd = _listing_id(db_path, "MTD", "US")
    k10 = _listing_id(db_path, "0K10", "LSE")
    asml = _listing_id(db_path, "ASML", "AS")

    reached = _resolve(db_path, [mtd])

    assert {mtd, k10} <= reached
    assert asml not in reached, "an unrelated security must not be pulled in"


def test_same_issuer_listings_are_neighbours(tmp_path: Path) -> None:
    """Splits need every current member of the group, not just the changed one."""

    db_path = tmp_path / "issuer-link.db"
    _catalog(db_path, "US", "AAA")
    _catalog(db_path, "F", "BBB")
    aaa = _listing_id(db_path, "AAA", "US")
    bbb = _listing_id(db_path, "BBB", "F")
    with sqlite3.connect(db_path) as conn:
        shared = conn.execute(
            "SELECT issuer_id FROM listing WHERE listing_id = ?", (aaa,)
        ).fetchone()[0]
        conn.execute(
            "UPDATE listing SET issuer_id = ? WHERE listing_id = ?", (shared, bbb)
        )

    assert {aaa, bbb} <= _resolve(db_path, [aaa])


def test_issuer_holding_the_payload_lei_is_a_neighbour(tmp_path: Path) -> None:
    """The entity a listing is about to join is reachable by neither other link.

    ``AAAY.US`` has its own ISIN and its own issuer, so only its payload's LEI
    connects it to the issuer already holding that LEI.
    """

    db_path = tmp_path / "lei-link.db"
    lei = "213800WCOWEI3T5DUV19"
    _catalog(db_path, "LSE", "AAA", "GB00B1CKQ739")
    _catalog(db_path, "US", "AAAY", "US26543P1030")
    FundamentalsRepository(db_path).initialize_schema()

    aaa = _listing_id(db_path, "AAA", "LSE")
    aaay = _listing_id(db_path, "AAAY", "US")
    with sqlite3.connect(db_path) as conn:
        issuer = conn.execute(
            "SELECT issuer_id FROM listing WHERE listing_id = ?", (aaa,)
        ).fetchone()[0]
        conn.execute("UPDATE issuer SET lei = ? WHERE issuer_id = ?", (lei, issuer))
        # Written straight to the table rather than through the ingest path,
        # which now reconciles as it stores and would merge these two before the
        # closure could be observed -- leaving the LEI link untested.
        conn.execute(
            """
            INSERT INTO fundamentals_raw (
                provider_listing_id, data, payload_hash, last_fetched_at
            )
            SELECT pl.provider_listing_id, ?, ?, '2026-01-01T00:00:00+00:00'
            FROM provider_listing pl WHERE pl.listing_id = ?
            """,
            ('{"General": {"LEI": "%s"}}' % lei, "0" * 64, aaay),
        )

    assert {aaa, aaay} <= _resolve(db_path, [aaay])


def test_closure_is_transitive(tmp_path: Path) -> None:
    """A -> B by issuer, B -> C by ISIN: seeding A must reach C.

    One hop would leave a partition disagreeing with a whole-universe pass,
    which is the order-dependence this exists to remove.
    """

    db_path = tmp_path / "transitive.db"
    _catalog(db_path, "US", "AAA")
    _catalog(db_path, "F", "BBB", "US5926881054")
    _catalog(db_path, "STU", "CCC", "US5926881054")

    aaa = _listing_id(db_path, "AAA", "US")
    bbb = _listing_id(db_path, "BBB", "F")
    ccc = _listing_id(db_path, "CCC", "STU")
    with sqlite3.connect(db_path) as conn:
        shared = conn.execute(
            "SELECT issuer_id FROM listing WHERE listing_id = ?", (aaa,)
        ).fetchone()[0]
        conn.execute(
            "UPDATE listing SET issuer_id = ? WHERE listing_id = ?", (shared, bbb)
        )

    assert {aaa, bbb, ccc} <= _resolve(db_path, [aaa])


def test_none_scope_returns_the_whole_universe(tmp_path: Path) -> None:
    """A whole-universe run is its own neighbourhood -- no closure work needed."""

    db_path = tmp_path / "universe.db"
    _catalog(db_path, "US", "AAA")
    _catalog(db_path, "F", "BBB")

    with sqlite3.connect(db_path) as conn:
        every = {int(row[0]) for row in conn.execute("SELECT listing_id FROM listing")}

    assert _resolve(db_path, None) == every


def test_empty_seed_stays_empty(tmp_path: Path) -> None:
    """An ingest batch that stored nothing must not expand to the universe."""

    db_path = tmp_path / "empty.db"
    _catalog(db_path, "US", "AAA")

    assert _resolve(db_path, []) == set()


def test_isolated_listing_is_its_own_neighbourhood(tmp_path: Path) -> None:
    """No identifiers, no links -- 18,043 live listings are in this state."""

    db_path = tmp_path / "isolated.db"
    _catalog(db_path, "BK", "NETBAY")
    _catalog(db_path, "US", "AAA", "US5926881054")
    netbay = _listing_id(db_path, "NETBAY", "BK")

    assert _resolve(db_path, [netbay]) == {netbay}


def test_round_cap_is_honoured(tmp_path: Path) -> None:
    """The cap bounds the walk rather than letting a bad chain run away.

    A chain of listings each sharing an issuer with the next needs one round per
    link; capping at 1 must stop early instead of resolving the whole chain.
    """

    db_path = tmp_path / "capped.db"
    isin_a, isin_b = "US0000000017", "US0000000025"
    # S0 -[isin A]- S1 -[issuer]- S2 -[isin B]- S3: each hop needs its own round,
    # because only newly reached listings can reach further.
    _catalog(db_path, "US", "S0", isin_a)
    _catalog(db_path, "F", "S1", isin_a)
    _catalog(db_path, "STU", "S2", isin_b)
    _catalog(db_path, "MU", "S3", isin_b)
    ids = [
        _listing_id(db_path, "S0", "US"),
        _listing_id(db_path, "S1", "F"),
        _listing_id(db_path, "S2", "STU"),
        _listing_id(db_path, "S3", "MU"),
    ]
    with sqlite3.connect(db_path) as conn:
        issuer_of_s1 = conn.execute(
            "SELECT issuer_id FROM listing WHERE listing_id = ?", (ids[1],)
        ).fetchone()[0]
        conn.execute(
            "UPDATE listing SET issuer_id = ? WHERE listing_id = ?",
            (issuer_of_s1, ids[2]),
        )

    with sqlite3.connect(db_path) as conn:
        one_round = resolve_neighbourhood(conn, [ids[0]], max_rounds=1)
        two_rounds = resolve_neighbourhood(conn, [ids[0]], max_rounds=2)
        uncapped = resolve_neighbourhood(conn, [ids[0]])

    assert one_round == {ids[0], ids[1]}
    assert two_rounds == {ids[0], ids[1], ids[2]}
    assert uncapped == set(ids)


def test_issuer_ids_for_returns_current_parents(tmp_path: Path) -> None:
    """The apply step scopes its orphan cleanup to these rows."""

    db_path = tmp_path / "issuer-ids.db"
    _catalog(db_path, "US", "AAA")
    _catalog(db_path, "F", "BBB")
    aaa = _listing_id(db_path, "AAA", "US")
    bbb = _listing_id(db_path, "BBB", "F")

    with sqlite3.connect(db_path) as conn:
        expected = {
            int(row[0])
            for row in conn.execute(
                "SELECT issuer_id FROM listing WHERE listing_id IN (?, ?)",
                (aaa, bbb),
            )
        }
        assert issuer_ids_for(conn, [aaa, bbb]) == expected
        assert issuer_ids_for(conn, []) == set()


def test_closure_queries_seek_indexes(tmp_path: Path) -> None:
    """No scans: this runs per ingest batch, ~2,900 times in a bootstrap.

    A plan regression here would be invisible in correctness terms and turn a
    per-batch index seek into a walk of the whole catalog.
    """

    from pyvalue.persistence.storage.universe_reconcile import (
        _PAYLOAD_LEI_SQL,
        _SAME_ISIN_SQL,
        _SAME_ISSUER_SQL,
    )

    db_path = tmp_path / "query-plans.db"
    _catalog(db_path, "US", "AAA", "US5926881054")

    with sqlite3.connect(db_path) as conn:
        for sql in (_SAME_ISIN_SQL, _SAME_ISSUER_SQL, _PAYLOAD_LEI_SQL):
            steps = [
                str(row[-1])
                for row in conn.execute(
                    "EXPLAIN QUERY PLAN " + sql.format(placeholders="?"), (1,)
                )
            ]
            assert steps, sql
            assert not [step for step in steps if step.startswith("SCAN")], steps
