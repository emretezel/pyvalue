"""Enterprise-value based valuation metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

import logging

from pyvalue.fx import FXRateStore
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.enterprise_value import (
    EV_FALLBACK_REQUIRED_CONCEPTS,
    merge_currency_codes,
    resolve_enterprise_value_denominator,
)
from pyvalue.money import ephemeral_fx_database_path, normalize_money_value
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}

EBIT_CONCEPT = "OperatingIncomeLoss"
OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
CAPEX_CONCEPT = "CapitalExpenditures"
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"

EBIT_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys((EBIT_CONCEPT,) + EV_FALLBACK_REQUIRED_CONCEPTS)
)
FCF_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (
            OPERATING_CASH_FLOW_CONCEPT,
            CAPEX_CONCEPT,
        )
        + EV_FALLBACK_REQUIRED_CONCEPTS
    )
)
EBITDA_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (
            EBIT_CONCEPT,
            DA_PRIMARY_CONCEPT,
            DA_FALLBACK_CONCEPT,
        )
        + EV_FALLBACK_REQUIRED_CONCEPTS
    )
)


@dataclass(frozen=True)
class _TTMResult:
    total: float
    as_of: str
    currency: Optional[str]


class EnterpriseValueRatioCalculator:
    """Shared numerator calculators for EV-based valuation metrics."""

    def compute_ttm_ebit(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_TTMResult]:
        return self._compute_ttm_amount(symbol, repo, EBIT_CONCEPT, context=context)

    def compute_ttm_fcf(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_TTMResult]:
        operating = self._compute_ttm_amount(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPT,
            context=context,
        )
        if operating is None:
            LOGGER.warning("%s: missing TTM FCF for %s", context, symbol)
            return None

        capex = self._compute_ttm_amount(
            symbol,
            repo,
            CAPEX_CONCEPT,
            context=context,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for %s; assuming zero", context, symbol
            )
            return _TTMResult(
                total=operating.total,
                as_of=operating.as_of,
                currency=operating.currency,
            )

        currency = merge_currency_codes([operating.currency, capex.currency])
        if currency is None and any(
            code is not None for code in (operating.currency, capex.currency)
        ):
            LOGGER.warning(
                "%s: currency conflict in TTM FCF inputs for %s", context, symbol
            )
            return None

        return _TTMResult(
            total=operating.total - capex.total,
            as_of=max(operating.as_of, capex.as_of),
            currency=currency,
        )

    def compute_ttm_ebitda(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_TTMResult]:
        ebit_records = self._filter_quarterly(
            repo.facts_for_concept(symbol, EBIT_CONCEPT)
        )
        if len(ebit_records) < 4:
            LOGGER.warning("%s: need 4 quarterly EBIT records for %s", context, symbol)
            return None
        if not is_recent_fact(ebit_records[0], max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest EBIT (%s) too old for %s",
                context,
                ebit_records[0].end_date,
                symbol,
            )
            return None

        da_primary = self._quarterly_map(
            repo.facts_for_concept(symbol, DA_PRIMARY_CONCEPT)
        )
        da_fallback = self._quarterly_map(
            repo.facts_for_concept(symbol, DA_FALLBACK_CONCEPT)
        )

        total = 0.0
        currency: Optional[str] = None
        for ebit_record in ebit_records[:4]:
            da_record = da_primary.get(ebit_record.end_date) or da_fallback.get(
                ebit_record.end_date
            )
            if da_record is None:
                LOGGER.warning(
                    "%s: missing D&A for quarter %s (%s)",
                    context,
                    ebit_record.end_date,
                    symbol,
                )
                return None

            ebit_value, ebit_currency = self._normalize_record(ebit_record)
            da_value, da_currency = self._normalize_record(da_record)
            merged = merge_currency_codes([currency, ebit_currency, da_currency])
            if merged is None and any(
                code is not None for code in (currency, ebit_currency, da_currency)
            ):
                LOGGER.warning(
                    "%s: currency conflict in EBIT/D&A for %s",
                    context,
                    symbol,
                )
                return None
            currency = merged
            total += ebit_value + da_value

        return _TTMResult(
            total=total,
            as_of=ebit_records[0].end_date,
            currency=currency,
        )

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
        *,
        context: str,
    ) -> Optional[_TTMResult]:
        quarterly = self._filter_quarterly(repo.facts_for_concept(symbol, concept))
        if len(quarterly) < 4:
            LOGGER.warning(
                "%s: need 4 quarterly %s records for %s, found %s",
                context,
                concept,
                symbol,
                len(quarterly),
            )
            return None
        if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest %s (%s) too old for %s",
                context,
                concept,
                quarterly[0].end_date,
                symbol,
            )
            return None

        normalized: list[float] = []
        currency: Optional[str] = None
        for record in quarterly[:4]:
            value, code = self._normalize_record(record)
            merged = merge_currency_codes([currency, code])
            if merged is None and any(
                existing is not None for existing in (currency, code)
            ):
                LOGGER.warning(
                    "%s: currency conflict in %s quarterly values for %s",
                    context,
                    concept,
                    symbol,
                )
                return None
            currency = merged
            normalized.append(value)

        return _TTMResult(
            total=sum(normalized),
            as_of=quarterly[0].end_date,
            currency=currency,
        )

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
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _quarterly_map(self, records: Sequence[FactRecord]) -> dict[str, FactRecord]:
        return {record.end_date: record for record in self._filter_quarterly(records)}

    def _normalize_record(self, record: FactRecord) -> tuple[float, Optional[str]]:
        normalized_value, normalized_currency = normalize_money_value(
            record.value,
            getattr(record, "currency", None),
        )
        return (
            record.value if normalized_value is None else normalized_value,
            normalized_currency,
        )


@dataclass
class EBITYieldEVMetric:
    """Compute trailing EBIT yield on enterprise value."""

    id: str = "ebit_yield_ev"
    required_concepts = EBIT_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_ebit(
            symbol, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning("%s: missing numerator for %s", self.id, symbol)
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            symbol=symbol,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
            converter=FXRateStore(
                str(getattr(repo, "db_path", ephemeral_fx_database_path()))
            ).convert,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.total / enterprise_value,
            as_of=numerator.as_of,
        )


@dataclass
class FCFYieldEVMetric:
    """Compute trailing FCF yield on enterprise value."""

    id: str = "fcf_yield_ev"
    required_concepts = FCF_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_fcf(
            symbol, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning("%s: missing numerator for %s", self.id, symbol)
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            symbol=symbol,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
            converter=FXRateStore(
                str(getattr(repo, "db_path", ephemeral_fx_database_path()))
            ).convert,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.total / enterprise_value,
            as_of=numerator.as_of,
        )


@dataclass
class EVToEBITMetric:
    """Compute enterprise value divided by trailing EBIT."""

    id: str = "ev_to_ebit"
    required_concepts = EBIT_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_ebit(
            symbol, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning("%s: missing denominator EBIT for %s", self.id, symbol)
            return None
        if numerator.total <= 0:
            LOGGER.warning("%s: non-positive EBIT for %s", self.id, symbol)
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            symbol=symbol,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
            converter=FXRateStore(
                str(getattr(repo, "db_path", ephemeral_fx_database_path()))
            ).convert,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=enterprise_value / numerator.total,
            as_of=numerator.as_of,
        )


@dataclass
class EVToEBITDAMetric:
    """Compute enterprise value divided by trailing component EBITDA."""

    id: str = "ev_to_ebitda"
    required_concepts = EBITDA_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_ebitda(
            symbol, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning("%s: missing denominator EBITDA for %s", self.id, symbol)
            return None
        if numerator.total <= 0:
            LOGGER.warning("%s: non-positive EBITDA for %s", self.id, symbol)
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            symbol=symbol,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
            converter=FXRateStore(
                str(getattr(repo, "db_path", ephemeral_fx_database_path()))
            ).convert,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=enterprise_value / numerator.total,
            as_of=numerator.as_of,
        )


__all__ = [
    "EBITYieldEVMetric",
    "FCFYieldEVMetric",
    "EVToEBITMetric",
    "EVToEBITDAMetric",
]
