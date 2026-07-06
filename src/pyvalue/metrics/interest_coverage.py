"""Interest coverage metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import (
    QUARTERLY_PERIODS,
    paired_records,
    resolve_ttm_window,
)
from pyvalue.metrics.utils import (
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

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
        ebit_records = repo.monetary_facts_for_concept(listing_id, EBIT_CONCEPTS[0])
        direct_interest = repo.monetary_facts_for_concept(
            listing_id, INTEREST_CONCEPTS[0]
        )
        result = self._compute_aligned(
            listing_id=listing_id,
            ebit_records=ebit_records,
            interest_records=direct_interest,
            log_failures=False,
            repo=repo,
        )
        if result is not None:
            return result

        fallback_interest = repo.monetary_facts_for_concept(
            listing_id, INTEREST_FALLBACK_CONCEPTS[0]
        )
        # Direct rows precede fallback rows: the aligned pairing keeps the
        # first candidate per end_date, so a direct quarter always beats the
        # derived fallback for the same date (the legacy merge rule).
        merged_interest = [*direct_interest, *fallback_interest]
        if fallback_interest:
            result = self._compute_aligned(
                listing_id=listing_id,
                ebit_records=ebit_records,
                interest_records=merged_interest,
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
        cap_result = self._debt_free_cap(listing_id, ebit_records, repo)
        if cap_result is not None:
            return cap_result

        # Genuine NA: emit the precise legacy warning so the persisted
        # failure reason stays diagnostic (missing interest facts entirely,
        # or the aligned-window/sign detail from a logged re-run).
        if not any(
            (record.fiscal_period or "").upper() in QUARTERLY_PERIODS
            for record in merged_interest
        ):
            LOGGER.warning(
                "interest_coverage: missing direct and fallback interest expense for listing_id=%s",
                listing_id,
            )
            return None
        return self._compute_aligned(
            listing_id=listing_id,
            ebit_records=ebit_records,
            interest_records=merged_interest,
            log_failures=True,
            repo=repo,
        )

    def _debt_free_cap(
        self,
        listing_id: int,
        ebit_records: Sequence[MonetaryFact],
        repo: RegionFactsRepository,
    ) -> Optional[MetricResult]:
        """Return the capped result when fresh TTM EBIT is positive.

        The cap deliberately keys on the EBIT-only quarterly series, not the
        EBIT-and-interest aligned one: for an issuer whose interest line ended
        years ago (the PLTR shape) the aligned series is stale while the
        business itself is current. Returns ``None`` when no fresh TTM window
        forms from the EBIT rows, or when TTM EBIT is non-positive -- those
        issuers stay NA.
        """

        resolution = resolve_ttm_window(ebit_records)
        window = resolution.window
        if window is None:
            return None

        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name=EBIT_CONCEPTS[0]
        )
        ebit_ttm = self._ttm_money(
            list(window.records), EBIT_CONCEPTS[0], target_currency, listing_id
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
            as_of=window.as_of,
        )

    def _compute_aligned(
        self,
        *,
        listing_id: int,
        ebit_records: Sequence[MonetaryFact],
        interest_records: Sequence[MonetaryFact],
        log_failures: bool,
        repo: RegionFactsRepository,
    ) -> Optional[MetricResult]:
        # The window resolves over the *aligned* series -- EBIT quarters that
        # have a same-date interest row -- exactly as the legacy
        # intersect-then-take-4 selection did. Resolving over EBIT alone and
        # then demanding interest on those dates would silently reroute
        # lagging-interest issuers from a measured ratio into the 100x cap.
        interest_dates = {
            record.end_date
            for record in interest_records
            if (record.fiscal_period or "").upper() in QUARTERLY_PERIODS
        }
        aligned_ebit = [
            record for record in ebit_records if record.end_date in interest_dates
        ]
        resolution = resolve_ttm_window(aligned_ebit)
        window = resolution.window
        if window is None:
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: %s (aligned EBIT/interest quarters, listing_id=%s)",
                    resolution.failure,
                    listing_id,
                )
            return None
        pairs = paired_records(window, interest_records)
        if pairs is None:
            # Unreachable by construction (every window date came from the
            # aligned series); kept as a defensive guard.
            return None

        # Align every quarter to the listing currency before summing, so the
        # EBIT/interest ratio (Money / Money) is currency-safe.
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name=EBIT_CONCEPTS[0]
        )
        ebit_ttm = self._ttm_money(
            [ebit for ebit, _ in pairs], EBIT_CONCEPTS[0], target_currency, listing_id
        )
        interest_ttm = self._ttm_money(
            [interest for _, interest in pairs],
            INTEREST_CONCEPTS[0],
            target_currency,
            listing_id,
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
        return MetricResult.ratio(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=window.as_of,
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


__all__ = ["INTEREST_COVERAGE_CAP", "InterestCoverageMetric"]
