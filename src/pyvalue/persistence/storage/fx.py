"""FX rate, FX supported-pair, and FX refresh-state repositories.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from typing import (
    List,
    Optional,
    Sequence,
)

from pyvalue.currency import (
    normalize_currency_code,
)

from .base import (
    SQLiteStore,
    _normalize_optional_text,
    _primary_listing_predicate,
    _utc_now_iso,
)
from .records import (
    FXRateRecord,
    FXRefreshStateRecord,
    FXSupportedPairRecord,
)
from ..migrations import apply_migrations


class FXRatesRepository(SQLiteStore):
    """Persist and query direct FX rate observations."""

    def initialize_schema(self) -> None:
        # `fx_rates` (table + idx_fx_rates_pair_date) is owned by migration 026.
        apply_migrations(self.db_path)

    def upsert(self, record: FXRateRecord) -> None:
        self.upsert_many([record])

    def upsert_many(self, records: Sequence[FXRateRecord]) -> int:
        self.initialize_schema()
        if not records:
            return 0
        now = _utc_now_iso()
        payload = [
            (
                record.provider.strip().upper(),
                record.rate_date,
                normalize_currency_code(record.base_currency),
                normalize_currency_code(record.quote_currency),
                float(record.rate),
                record.fetched_at,
                record.source_kind.strip().lower(),
                record.meta_json,
                record.created_at or now,
                record.updated_at or now,
            )
            for record in records
            if normalize_currency_code(record.base_currency)
            and normalize_currency_code(record.quote_currency)
        ]
        if not payload:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO fx_rates (
                    provider,
                    rate_date,
                    base_currency,
                    quote_currency,
                    rate,
                    fetched_at,
                    source_kind,
                    meta_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, rate_date, base_currency, quote_currency)
                DO UPDATE SET
                    rate = excluded.rate,
                    fetched_at = excluded.fetched_at,
                    source_kind = excluded.source_kind,
                    meta_json = excluded.meta_json,
                    updated_at = excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def latest_on_or_before(
        self,
        provider: str,
        base_currency: str,
        quote_currency: str,
        as_of: str,
    ) -> Optional[FXRateRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    provider,
                    rate_date,
                    base_currency,
                    quote_currency,
                    rate,
                    fetched_at,
                    source_kind,
                    meta_json,
                    created_at,
                    updated_at
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND quote_currency = ?
                  AND rate_date <= ?
                ORDER BY rate_date DESC
                LIMIT 1
                """,
                (
                    provider.strip().upper(),
                    normalize_currency_code(base_currency),
                    normalize_currency_code(quote_currency),
                    as_of,
                ),
            ).fetchone()
        if row is None:
            return None
        return FXRateRecord(*row)

    def fetch_pair_history(
        self,
        provider: str,
        base_currency: str,
        quote_currency: str,
    ) -> list[tuple[str, float]]:
        """Return one direct pair history ordered by ascending rate date."""

        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT rate_date, rate
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND quote_currency = ?
                ORDER BY rate_date ASC
                """,
                (
                    provider.strip().upper(),
                    normalize_currency_code(base_currency),
                    normalize_currency_code(quote_currency),
                ),
            ).fetchall()
        return [(str(row["rate_date"]), float(row["rate"])) for row in rows]

    def fetch_all_for_provider(
        self,
        provider: str,
    ) -> list[tuple[str, str, str, float]]:
        """Return the full direct-rate history for one provider."""

        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT base_currency, quote_currency, rate_date, rate
                FROM fx_rates
                WHERE provider = ?
                ORDER BY base_currency ASC, quote_currency ASC, rate_date ASC
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [
            (
                str(row["base_currency"]),
                str(row["quote_currency"]),
                str(row["rate_date"]),
                float(row["rate"]),
            )
            for row in rows
        ]

    def pair_coverage(
        self,
        provider: str,
        base_currency: str,
        quote_currency: str,
    ) -> tuple[Optional[str], Optional[str]]:
        """Return min/max stored direct dates for one pair."""

        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(rate_date) AS min_rate_date, MAX(rate_date) AS max_rate_date
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND quote_currency = ?
                """,
                (
                    provider.strip().upper(),
                    normalize_currency_code(base_currency),
                    normalize_currency_code(quote_currency),
                ),
            ).fetchone()
        if row is None:
            return None, None
        return (
            _normalize_optional_text(row["min_rate_date"]),
            _normalize_optional_text(row["max_rate_date"]),
        )

    def fully_covered_quotes_for_window(
        self,
        provider: str,
        base_currency: str,
        quote_currencies: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> set[str]:
        """Return quotes whose direct rows fully cover one inclusive date window.

        The refresh command only skips a base/quote window when the stored rows
        cover every day in that exact requested window. Sparse historical rows
        must not be treated as continuous coverage just because their min/max
        dates span the window.
        """

        self.initialize_schema()
        normalized_quotes = [
            code
            for code in (
                normalize_currency_code(quote_currency)
                for quote_currency in quote_currencies
            )
            if code is not None
        ]
        if not normalized_quotes:
            return set()
        expected_days = (end_date - start_date).days + 1
        if expected_days <= 0:
            return set()
        placeholders = ", ".join("?" for _ in normalized_quotes)
        params = [
            provider.strip().upper(),
            normalize_currency_code(base_currency),
            start_date.isoformat(),
            end_date.isoformat(),
            *normalized_quotes,
        ]
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    quote_currency,
                    COUNT(*) AS row_count,
                    MIN(rate_date) AS min_rate_date,
                    MAX(rate_date) AS max_rate_date
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND rate_date >= ?
                  AND rate_date <= ?
                  AND quote_currency IN ({placeholders})
                GROUP BY quote_currency
                """,
                params,
            ).fetchall()
        return {
            str(row["quote_currency"])
            for row in rows
            if row["min_rate_date"] == start_date.isoformat()
            and row["max_rate_date"] == end_date.isoformat()
            and int(row["row_count"]) == expected_days
        }

    def discover_currencies(self) -> List[str]:
        """Return distinct normalized currencies referenced by the project DB."""

        self.initialize_schema()
        currencies: set[str] = set()
        with self._connect() as conn:
            supported_rows = conn.execute(
                f"""
                SELECT DISTINCT st.currency
                FROM supported_tickers st
                WHERE st.currency IS NOT NULL
                  AND {_primary_listing_predicate("st")}
                ORDER BY st.currency
                """
            ).fetchall()
            for row in supported_rows:
                code = normalize_currency_code(row["currency"])
                if code is not None:
                    currencies.add(code)
            rows = conn.execute(
                """
                SELECT DISTINCT currency
                FROM financial_facts
                WHERE currency IS NOT NULL
                ORDER BY currency
                """
            ).fetchall()
            for row in rows:
                code = normalize_currency_code(row["currency"])
                if code is not None:
                    currencies.add(code)
        return sorted(currencies)


class FXSupportedPairsRepository(SQLiteStore):
    """Persist FX provider catalog entries."""

    def initialize_schema(self) -> None:
        # `fx_supported_pairs` (table + idx_fx_supported_pairs_refreshable)
        # is owned by migration 028.
        apply_migrations(self.db_path)

    def replace_provider_catalog(
        self,
        provider: str,
        records: Sequence[FXSupportedPairRecord],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = _utc_now_iso()
        rows = [
            (
                provider_norm,
                record.symbol.strip().upper(),
                record.canonical_symbol.strip().upper(),
                normalize_currency_code(record.base_currency),
                normalize_currency_code(record.quote_currency),
                _normalize_optional_text(record.name),
                1 if record.is_alias else 0,
                1 if record.is_refreshable else 0,
                record.last_seen_at or now,
            )
            for record in records
            if record.symbol and record.canonical_symbol
        ]
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM fx_supported_pairs WHERE provider = ?",
                (provider_norm,),
            )
            if rows:
                conn.executemany(
                    """
                    INSERT INTO fx_supported_pairs (
                        provider,
                        symbol,
                        canonical_symbol,
                        base_currency,
                        quote_currency,
                        name,
                        is_alias,
                        is_refreshable,
                        last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
        return len(rows)

    def list_refreshable(self, provider: str) -> list[FXSupportedPairRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    provider,
                    symbol,
                    canonical_symbol,
                    base_currency,
                    quote_currency,
                    name,
                    is_alias,
                    is_refreshable,
                    last_seen_at
                FROM fx_supported_pairs
                WHERE provider = ?
                  AND is_refreshable = 1
                ORDER BY canonical_symbol ASC
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [
            FXSupportedPairRecord(
                provider=str(row["provider"]),
                symbol=str(row["symbol"]),
                canonical_symbol=str(row["canonical_symbol"]),
                base_currency=_normalize_optional_text(row["base_currency"]),
                quote_currency=_normalize_optional_text(row["quote_currency"]),
                name=_normalize_optional_text(row["name"]),
                is_alias=bool(row["is_alias"]),
                is_refreshable=bool(row["is_refreshable"]),
                last_seen_at=_normalize_optional_text(row["last_seen_at"]),
            )
            for row in rows
        ]


class FXRefreshStateRepository(SQLiteStore):
    """Persist FX refresh coverage and retry state per canonical symbol."""

    def initialize_schema(self) -> None:
        # `fx_refresh_state` is owned by migration 028.
        apply_migrations(self.db_path)

    def fetch(
        self,
        provider: str,
        canonical_symbol: str,
    ) -> Optional[FXRefreshStateRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    provider,
                    canonical_symbol,
                    min_rate_date,
                    max_rate_date,
                    full_history_backfilled,
                    last_fetched_at,
                    last_status,
                    last_error,
                    attempts
                FROM fx_refresh_state
                WHERE provider = ? AND canonical_symbol = ?
                """,
                (provider.strip().upper(), canonical_symbol.strip().upper()),
            ).fetchone()
        if row is None:
            return None
        return FXRefreshStateRecord(
            provider=str(row["provider"]),
            canonical_symbol=str(row["canonical_symbol"]),
            min_rate_date=_normalize_optional_text(row["min_rate_date"]),
            max_rate_date=_normalize_optional_text(row["max_rate_date"]),
            full_history_backfilled=bool(row["full_history_backfilled"]),
            last_fetched_at=_normalize_optional_text(row["last_fetched_at"]),
            last_status=_normalize_optional_text(row["last_status"]),
            last_error=_normalize_optional_text(row["last_error"]),
            attempts=int(row["attempts"] or 0),
        )

    def mark_success(
        self,
        provider: str,
        canonical_symbol: str,
        *,
        min_rate_date: Optional[str],
        max_rate_date: Optional[str],
        full_history_backfilled: bool,
        fetched_at: Optional[str] = None,
    ) -> None:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        symbol_norm = canonical_symbol.strip().upper()
        timestamp = fetched_at or _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider,
                    canonical_symbol,
                    min_rate_date,
                    max_rate_date,
                    full_history_backfilled,
                    last_fetched_at,
                    last_status,
                    last_error,
                    attempts
                ) VALUES (?, ?, ?, ?, ?, ?, 'ok', NULL, 0)
                ON CONFLICT(provider, canonical_symbol) DO UPDATE SET
                    min_rate_date = excluded.min_rate_date,
                    max_rate_date = excluded.max_rate_date,
                    full_history_backfilled = excluded.full_history_backfilled,
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'ok',
                    last_error = NULL,
                    attempts = 0
                """,
                (
                    provider_norm,
                    symbol_norm,
                    min_rate_date,
                    max_rate_date,
                    1 if full_history_backfilled else 0,
                    timestamp,
                ),
            )

    def mark_failure(
        self,
        provider: str,
        canonical_symbol: str,
        error: str,
    ) -> None:
        self.initialize_schema()
        state = self.fetch(provider, canonical_symbol)
        attempts = 1 if state is None else state.attempts + 1
        provider_norm = provider.strip().upper()
        symbol_norm = canonical_symbol.strip().upper()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider,
                    canonical_symbol,
                    min_rate_date,
                    max_rate_date,
                    full_history_backfilled,
                    last_fetched_at,
                    last_status,
                    last_error,
                    attempts
                ) VALUES (?, ?, NULL, NULL, 0, ?, 'error', ?, ?)
                ON CONFLICT(provider, canonical_symbol) DO UPDATE SET
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'error',
                    last_error = excluded.last_error,
                    attempts = excluded.attempts
                """,
                (
                    provider_norm,
                    symbol_norm,
                    _utc_now_iso(),
                    str(error),
                    attempts,
                ),
            )
