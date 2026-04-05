"""Net buyback yield metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.fx import FXRateStore
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.share_count_change import ShareCountChangeCalculator
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.money import ephemeral_fx_database_path
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)

SALE_PURCHASE_CONCEPT = "SalePurchaseOfStock"
ISSUANCE_CAPITAL_STOCK_CONCEPT = "IssuanceOfCapitalStock"
SHARE_COUNT_CONCEPT = "CommonStockSharesOutstanding"

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FALLBACK_YEARS = 1

REQUIRED_CONCEPTS = (
    SALE_PURCHASE_CONCEPT,
    ISSUANCE_CAPITAL_STOCK_CONCEPT,
    SHARE_COUNT_CONCEPT,
)


@dataclass(frozen=True)
class _TTMResult:
    total: float
    as_of: str
    currency: Optional[str]


def _convert_market_cap(
    *,
    symbol: str,
    amount: float,
    source_currency: Optional[str],
    target_currency: Optional[str],
    as_of: str,
    context: str,
    database: str,
) -> Optional[float]:
    if source_currency and target_currency and source_currency != target_currency:
        converted = FXRateStore(database).convert(
            amount,
            source_currency,
            target_currency,
            as_of,
        )
        if converted is None:
            LOGGER.warning(
                "%s: FX conversion failed %s -> %s for %s",
                context,
                source_currency,
                target_currency,
                symbol,
            )
            return None
        return converted
    return amount


@dataclass
class NetBuybackYieldMetric:
    """Compute net buyback yield using EODHD financing cash flow or share fallback."""

    id: str = "net_buyback_yield"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = self._compute_net_buybacks_ttm(symbol, repo)
        if numerator is not None:
            market_cap = self._market_cap_denominator(
                symbol=symbol,
                market_repo=market_repo,
                target_currency=numerator.currency,
            )
            if market_cap is not None:
                return MetricResult(
                    symbol=symbol,
                    metric_id=self.id,
                    value=numerator.total / market_cap,
                    as_of=numerator.as_of,
                )

        fallback = self._share_count_fallback(symbol, repo)
        if fallback is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=fallback.total,
            as_of=fallback.as_of,
        )

    def _compute_net_buybacks_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_TTMResult]:
        primary = self._ttm_sum(symbol, repo, SALE_PURCHASE_CONCEPT)
        if primary is not None:
            return _TTMResult(
                total=-primary.total,
                as_of=primary.as_of,
                currency=primary.currency,
            )

        fallback = self._ttm_sum(symbol, repo, ISSUANCE_CAPITAL_STOCK_CONCEPT)
        if fallback is not None:
            return _TTMResult(
                total=-fallback.total,
                as_of=fallback.as_of,
                currency=fallback.currency,
            )
        return None

    def _ttm_sum(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
    ) -> Optional[_TTMResult]:
        records = repo.facts_for_concept(symbol, concept)
        quarterly = self._filter_quarterly(records)
        if len(quarterly) < 4:
            LOGGER.warning(
                "%s: need 4 quarterly %s records for %s, found %s",
                self.id,
                concept,
                symbol,
                len(quarterly),
            )
            return None
        if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest %s (%s) too old for %s",
                self.id,
                concept,
                quarterly[0].end_date,
                symbol,
            )
            return None

        normalized, currency = self._normalize_quarterly(quarterly[:4])
        if normalized is None:
            LOGGER.warning(
                "%s: currency conflict in %s quarterly values for %s",
                self.id,
                concept,
                symbol,
            )
            return None

        return _TTMResult(
            total=sum(record.value for record in normalized),
            as_of=normalized[0].end_date,
            currency=currency,
        )

    def _share_count_fallback(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_TTMResult]:
        snapshot = ShareCountChangeCalculator().compute_pair_for_years(
            symbol,
            repo,
            exact_years=FALLBACK_YEARS,
            context=self.id,
        )
        if snapshot is None:
            return None
        value = -((snapshot.latest.shares / snapshot.prior.shares) - 1.0)
        return _TTMResult(total=value, as_of=snapshot.as_of, currency=None)

    def _market_cap_denominator(
        self,
        *,
        symbol: str,
        market_repo: MarketDataRepository,
        target_currency: Optional[str],
    ) -> Optional[float]:
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot is None or snapshot.market_cap is None:
            LOGGER.warning("%s: missing market cap snapshot for %s", self.id, symbol)
            return None
        if snapshot.market_cap <= 0:
            LOGGER.warning(
                "%s: non-positive market cap snapshot for %s",
                self.id,
                symbol,
            )
            return None

        converted = _convert_market_cap(
            symbol=symbol,
            amount=snapshot.market_cap,
            source_currency=getattr(snapshot, "currency", None),
            target_currency=target_currency,
            as_of=snapshot.as_of,
            context=self.id,
            database=str(getattr(market_repo, "db_path", ephemeral_fx_database_path())),
        )
        if converted is None:
            return None
        if converted <= 0:
            LOGGER.warning(
                "%s: non-positive market cap after FX for %s",
                self.id,
                symbol,
            )
            return None
        return converted

    def _filter_quarterly(self, records: Iterable[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _normalize_quarterly(
        self, records: list[FactRecord]
    ) -> tuple[Optional[list[FactRecord]], Optional[str]]:
        currency = None
        normalized: list[FactRecord] = []
        for record in records:
            code = getattr(record, "currency", None)
            value = record.value
            if code in {"GBX", "GBP0.01"}:
                code = "GBP"
                value = value / 100.0 if value is not None else None
            if value is None:
                continue
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(
                FactRecord(
                    symbol=record.symbol,
                    cik=record.cik,
                    concept=record.concept,
                    fiscal_period=record.fiscal_period,
                    end_date=record.end_date,
                    unit=record.unit,
                    value=value,
                    accn=record.accn,
                    filed=record.filed,
                    frame=record.frame,
                    start_date=getattr(record, "start_date", None),
                    accounting_standard=getattr(record, "accounting_standard", None),
                    currency=code,
                )
            )
        return normalized, currency


__all__ = ["NetBuybackYieldMetric"]
