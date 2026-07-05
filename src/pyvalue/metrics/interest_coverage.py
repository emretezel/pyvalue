"""Interest coverage metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
EBIT_CONCEPTS = ("OperatingIncomeLoss",)
INTEREST_CONCEPTS = ("InterestExpense",)
INTEREST_FALLBACK_CONCEPTS = ("InterestExpenseFromNetInterestIncome",)

# Documented cap emitted when coverage is economically unbounded: fresh,
# positive TTM EBIT with no measurable interest expense (line never reported,
# stale after a debt repayment, or non-positive). Debt-free issuers stop
# reporting an interest line entirely, so a plain ratio can never pass a
# `>= N` screen gate for exactly the strongest balance sheets. 100x sits
# above every screen threshold in use and is a *convention*, not a
# measurement -- see docs/reference/metrics.md.
INTEREST_COVERAGE_CAP: float = 100.0


@dataclass
class InterestCoverageMetric:
    """Compute TTM interest coverage (EBIT / interest expense)."""

    id: str = "interest_coverage"
    required_concepts = tuple(
        EBIT_CONCEPTS + INTEREST_CONCEPTS + INTEREST_FALLBACK_CONCEPTS
    )

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        ebit_quarters = self._quarterly_map(
            repo.monetary_facts_for_concept(listing_id, EBIT_CONCEPTS[0])
        )
        direct_interest_quarters = self._quarterly_map(
            repo.monetary_facts_for_concept(listing_id, INTEREST_CONCEPTS[0])
        )
        result = self._compute_from_maps(
            listing_id=listing_id,
            ebit_quarters=ebit_quarters,
            interest_quarters=direct_interest_quarters,
            log_failures=False,
            repo=repo,
        )
        if result is not None:
            return result

        fallback_interest_quarters = self._quarterly_map(
            repo.monetary_facts_for_concept(listing_id, INTEREST_FALLBACK_CONCEPTS[0])
        )
        merged_interest_quarters = dict(direct_interest_quarters)
        for end_date, record in fallback_interest_quarters.items():
            merged_interest_quarters.setdefault(end_date, record)
        if fallback_interest_quarters:
            result = self._compute_from_maps(
                listing_id=listing_id,
                ebit_quarters=ebit_quarters,
                interest_quarters=merged_interest_quarters,
                log_failures=False,
                repo=repo,
            )
            if result is not None:
                return result

        # The ratio path is exhausted: interest is absent, stale, misaligned,
        # or non-positive. For a business whose *current* TTM EBIT is positive
        # and fresh that is the debt-free/net-cash shape (an issuer that
        # repaid its debt typically stops reporting the interest line), so
        # coverage is economically unbounded -- emit the documented cap
        # instead of NA. Loss-making or stale-EBIT issuers are not rescued.
        cap_result = self._debt_free_cap(listing_id, ebit_quarters, repo)
        if cap_result is not None:
            return cap_result

        # Genuine NA: emit the precise legacy warning so the persisted
        # failure reason stays diagnostic (missing interest facts entirely,
        # or the aligned-quarter/freshness/sign detail from a logged re-run).
        if not merged_interest_quarters:
            LOGGER.warning(
                "interest_coverage: missing direct and fallback interest expense for listing_id=%s",
                listing_id,
            )
            return None
        return self._compute_from_maps(
            listing_id=listing_id,
            ebit_quarters=ebit_quarters,
            interest_quarters=merged_interest_quarters,
            log_failures=True,
            repo=repo,
        )

    def _debt_free_cap(
        self,
        listing_id: int,
        ebit_quarters: dict[str, MonetaryFact],
        repo: RegionFactsRepository,
    ) -> Optional[MetricResult]:
        """Return the capped result when fresh TTM EBIT is positive.

        The cap deliberately keys on the EBIT-only quarterly series, not the
        EBIT-and-interest aligned one: for an issuer whose interest line ended
        years ago (the PLTR shape) the aligned series is stale while the
        business itself is current. Returns ``None`` when EBIT is short,
        stale, or non-positive -- those issuers stay NA.
        """

        latest_dates = sorted(ebit_quarters, reverse=True)[:4]
        if len(latest_dates) < 4:
            return None
        ebit_records = [ebit_quarters[end_date] for end_date in latest_dates]
        if not is_recent_fact(ebit_records[0]):
            return None

        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name=EBIT_CONCEPTS[0]
        )
        ebit_ttm = self._ttm_money(
            ebit_records, EBIT_CONCEPTS[0], target_currency, listing_id
        )
        if ebit_ttm.amount <= 0:
            return None

        LOGGER.info(
            "interest_coverage: no measurable interest expense with positive TTM "
            "EBIT for listing_id=%s -- emitting documented cap %.0fx",
            listing_id,
            INTEREST_COVERAGE_CAP,
        )
        return MetricResult.ratio(
            listing_id=listing_id,
            metric_id=self.id,
            value=INTEREST_COVERAGE_CAP,
            as_of=ebit_records[0].end_date,
        )

    def _compute_from_maps(
        self,
        *,
        listing_id: int,
        ebit_quarters: dict[str, MonetaryFact],
        interest_quarters: dict[str, MonetaryFact],
        log_failures: bool,
        repo: RegionFactsRepository,
    ) -> Optional[MetricResult]:
        common_dates = sorted(
            set(ebit_quarters).intersection(interest_quarters), reverse=True
        )
        if len(common_dates) < 4:
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: need 4 aligned quarterly records for listing_id=%s",
                    listing_id,
                )
            return None
        common_dates = common_dates[:4]

        ebit_records = [ebit_quarters[end_date] for end_date in common_dates]
        interest_records = [interest_quarters[end_date] for end_date in common_dates]
        if not is_recent_fact(ebit_records[0]) or not is_recent_fact(
            interest_records[0]
        ):
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: latest quarter too old for listing_id=%s",
                    listing_id,
                )
            return None

        # Align every quarter to the listing currency before summing, so the
        # EBIT/interest ratio (Money / Money) is currency-safe.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name=EBIT_CONCEPTS[0]
        )
        ebit_ttm = self._ttm_money(
            ebit_records, EBIT_CONCEPTS[0], target_currency, listing_id
        )
        interest_ttm = self._ttm_money(
            interest_records, INTEREST_CONCEPTS[0], target_currency, listing_id
        )
        if ebit_ttm.amount <= 0:
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: non-positive EBIT for listing_id=%s", listing_id
                )
            return None
        if interest_ttm.amount <= 0:
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: non-positive interest expense for listing_id=%s",
                    listing_id,
                )
            return None

        ratio = ebit_ttm / interest_ttm
        as_of = max(ebit_records[0].end_date, interest_records[0].end_date)
        return MetricResult.ratio(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
        )

    def _ttm_money(
        self,
        records: Sequence[MonetaryFact],
        concept: str,
        target_currency: str,
        listing_id: int,
    ) -> Money:
        monies = [
            require_metric_money(
                record.money,
                target_currency=target_currency,
                metric_id=self.id,
                listing_id=listing_id,
                input_name=concept,
                as_of=record.end_date,
            )
            for record in records
        ]
        return sum_money(monies)

    def _quarterly_map(
        self, records: Sequence[MonetaryFact]
    ) -> dict[str, MonetaryFact]:
        # Keep the latest record per end_date to align quarters across concepts.
        ordered = self._filter_quarterly(records)
        return {record.end_date: record for record in ordered}

    def _filter_quarterly(self, records: Sequence[MonetaryFact]) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered


__all__ = ["INTEREST_COVERAGE_CAP", "InterestCoverageMetric"]
