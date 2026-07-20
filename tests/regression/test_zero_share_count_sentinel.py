"""Regression: EODHD zero-share sentinels never become canonical count facts.

The 2026-07 normalize-fundamentals log audit found 1,051 warnings of the form
"CommonStockSharesOutstanding (snapshot): missing General.UpdatedAt for X;
skipping" -- every one a dead-frontier-symbol skeleton payload (Karachi,
Egypt, US OTC, Nairobi, ...) carrying ``SharesStats.SharesOutstanding = 0``
and ``SharesFloat = 0`` with no ``General.UpdatedAt``. The warning blamed the
missing date, but the real issue was that 0 is EODHD's "no data" sentinel:
the normalizer treated only ``None`` as absent, so 24,307 non-positive
``unit_kind='count'`` rows (23,942 zeros, 365 negatives) accumulated in
``financial_facts`` through the statement and outstandingShares paths, and a
skeleton that ever gained an ``UpdatedAt`` would have stored a 0.0 INSTANT
count -- a division-by-zero landmine for every per-share metric.

The headline test replays the dated-skeleton shape: pre-fix it stores a 0.0
INSTANT ``CommonStockSharesOutstanding`` fact, post-fix it stores nothing.
Migration 086 purges the rows already stored.

Author: Emre Tezel
"""

from __future__ import annotations

import pytest

from pyvalue.normalization.eodhd import EODHDFactsNormalizer


def test_dated_skeleton_never_stores_zero_instant_share_count() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "SharesStats": {"SharesOutstanding": 0, "SharesFloat": 0},
        "General": {"CurrencyCode": "USD", "UpdatedAt": "2026-07-15"},
    }

    records = normalizer.normalize(payload, symbol="UOGPF.US")

    # Pre-fix: one INSTANT CommonStockSharesOutstanding fact with value 0.0.
    assert records == []


def test_undated_skeleton_normalizes_silently_to_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "SharesStats": {"SharesOutstanding": 0, "SharesFloat": 0},
        "General": {"CurrencyCode": "MUR"},
    }

    with caplog.at_level("WARNING"):
        records = normalizer.normalize(payload, symbol="ZWTO.SEM")

    assert records == []
    # The 1,051 audit warnings blamed the missing UpdatedAt; the sentinel
    # guard must bail first so the log stays clean for real anomalies.
    assert caplog.text == ""
