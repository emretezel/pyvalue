"""Interest coverage metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.balance_sheet import (
    DEBT_EVIDENCE_CONCEPTS,
    TOTAL_LIABILITIES_CONCEPT,
    resolve_debt_evidence,
    resolve_total_liabilities,
)
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import (
    QUARTERLY_PERIODS,
    paired_records,
    resolve_ttm_window,
)
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    filter_unique_fy,
    is_recent_fact,
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
# stale after a debt repayment, or non-positive) AND fresh balance-sheet
# evidence that debt is immaterial. Debt-free issuers stop reporting an
# interest line entirely, so a plain ratio can never pass a `>= N` screen
# gate for exactly the strongest balance sheets -- but a missing interest
# line alone is also the signature of a provider data gap on a levered
# issuer (the 2026-07-06 audit found 55% of cap-path listings carrying fresh
# debt above 1x TTM EBIT), so the cap demands corroboration instead of
# assuming debt-freedom. 100x sits above every screen threshold in use and
# is a *convention*, not a measurement -- see docs/reference/metrics.md.
INTEREST_COVERAGE_CAP: float = 100.0

# Cap eligibility bound: fresh debt evidence (or, failing that, total
# liabilities) must not exceed this multiple of TTM EBIT, inclusively.
# Derivation: at a punitive 10% assumed rate on debt equal to 1x EBIT the
# implied true coverage is still >= 10x -- above the toughest 6x gate in use
# (QARP) -- so an issuer under the bound passes any gate honestly even if it
# secretly pays interest. The slack over a strict zero test is deliberate:
# EODHD debt fields conflate leases and derived noncurrent liabilities with
# borrowings (web-verified debt-free PLTR.US shows 0.29x), so a tight bound
# would NA exactly the balance sheets the cap exists for.
CAP_MAX_DEBT_TO_EBIT: float = 1.0


@dataclass(frozen=True)
class _FreshTTMEbit:
    """Fresh, positive TTM EBIT resolved over the EBIT-only quarterly series."""

    money: Money
    as_of: str
    target_currency: str


@dataclass
class InterestCoverageMetric:
    """Compute TTM interest coverage (EBIT / interest expense)."""

    id: str = "interest_coverage"
    required_concepts = tuple(
        EBIT_CONCEPTS
        + INTEREST_CONCEPTS
        + INTEREST_FALLBACK_CONCEPTS
        + DEBT_EVIDENCE_CONCEPTS
        + (TOTAL_LIABILITIES_CONCEPT,)
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
        # and fresh that is *either* the debt-free/net-cash shape (an issuer
        # that repaid its debt typically stops reporting the interest line)
        # *or* a provider data gap on a levered issuer -- the balance sheet
        # arbitrates. Cap-or-veto is the final word for this branch: falling
        # through to the legacy missing-interest diagnostics below would
        # mislabel an evidence veto. Loss-making or stale-EBIT issuers are
        # not rescued.
        ebit_ttm = self._fresh_positive_ttm_ebit(listing_id, ebit_records, repo)
        if ebit_ttm is not None:
            return self._debt_free_cap(
                listing_id,
                ebit_ttm,
                repo,
                ebit_records=ebit_records,
                interest_records=merged_interest,
            )

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

    def _fresh_positive_ttm_ebit(
        self,
        listing_id: int,
        ebit_records: Sequence[MonetaryFact],
        repo: RegionFactsRepository,
    ) -> Optional[_FreshTTMEbit]:
        """Resolve fresh, positive TTM EBIT over the EBIT-only series.

        Deliberately keys on the EBIT-only quarterly series, not the
        EBIT-and-interest aligned one: for an issuer whose interest line
        ended years ago (the PLTR shape) the aligned series is stale while
        the business itself is current. Returns ``None`` when no fresh TTM
        window forms from the EBIT rows, or when TTM EBIT is non-positive --
        those issuers stay NA.
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
        return _FreshTTMEbit(
            money=ebit_ttm, as_of=window.as_of, target_currency=target_currency
        )

    def _debt_free_cap(
        self,
        listing_id: int,
        ebit: _FreshTTMEbit,
        repo: RegionFactsRepository,
        *,
        ebit_records: Sequence[MonetaryFact],
        interest_records: Sequence[MonetaryFact],
    ) -> Optional[MetricResult]:
        """Emit the cap only with evidence that debt is immaterial.

        A missing quarterly interest line on a fresh, profitable business is
        ambiguous: genuinely debt-free issuers stop reporting it, but provider
        feeds also drop it for levered issuers. Fresh balance-sheet debt
        evidence (upper bound; total liabilities as last resort) at or under
        ``CAP_MAX_DEBT_TO_EBIT`` x TTM EBIT proves the cap safe. When the
        evidence instead shows *material* debt (or is absent) the cap is
        unsafe -- but the issuer may still report interest annually even when
        it drops the quarterly line, so before conceding NA we try the annual
        ratio (:meth:`_fy_coverage`). The debt-evidence gate stays in front of
        that fallback deliberately: annual interest is noise-dominated when
        debt is small (contaminated with FX/financial charges, it can imply an
        absurd triple-digit rate), so a proven-immaterial-debt issuer must
        keep its cap and never be re-scored on that unreliable line.
        """

        threshold = ebit.money * CAP_MAX_DEBT_TO_EBIT

        evidence = resolve_debt_evidence(
            listing_id, repo, target_currency=ebit.target_currency, metric_id=self.id
        )
        if evidence is not None:
            if evidence.money > threshold:
                fy = self._fy_coverage(listing_id, ebit, ebit_records, interest_records)
                if fy is not None:
                    return fy
                LOGGER.warning(
                    "interest_coverage: material debt without measurable interest "
                    "expense for listing_id=%s -- provider data gap, not debt-free "
                    "(debt evidence %.0f %s > %.1fx TTM EBIT)",
                    listing_id,
                    evidence.money.amount,
                    evidence.money.currency,
                    CAP_MAX_DEBT_TO_EBIT,
                )
                return None
            # Explicit zero-debt rows and immaterial (lease/derived
            # contaminated) balances land here -- the genuine debt-free shape.
            return self._emit_cap(listing_id, ebit)

        liabilities = resolve_total_liabilities(
            listing_id, repo, target_currency=ebit.target_currency, metric_id=self.id
        )
        if liabilities is not None:
            if liabilities.money > threshold:
                fy = self._fy_coverage(listing_id, ebit, ebit_records, interest_records)
                if fy is not None:
                    return fy
                LOGGER.warning(
                    "interest_coverage: no fresh debt facts and material total "
                    "liabilities for listing_id=%s -- debt is unknown, not zero "
                    "(liabilities %.0f %s > %.1fx TTM EBIT)",
                    listing_id,
                    liabilities.money.amount,
                    liabilities.money.currency,
                    CAP_MAX_DEBT_TO_EBIT,
                )
                return None
            # Liabilities upper-bound debt, so a tiny balance sheet is
            # positive evidence even when every debt field is null.
            return self._emit_cap(listing_id, ebit)

        fy = self._fy_coverage(listing_id, ebit, ebit_records, interest_records)
        if fy is not None:
            return fy
        LOGGER.warning(
            "interest_coverage: no fresh balance-sheet evidence to support the "
            "debt-free cap for listing_id=%s",
            listing_id,
        )
        return None

    def _fy_coverage(
        self,
        listing_id: int,
        ebit: _FreshTTMEbit,
        ebit_records: Sequence[MonetaryFact],
        interest_records: Sequence[MonetaryFact],
    ) -> Optional[MetricResult]:
        """Measure coverage from the annual (FY) statements, or ``None``.

        The quarterly ratio path only consumes ``Q1``..``Q4`` interest rows, so
        an issuer that reports operating income quarterly but interest only in
        its annual filing (common for e.g. Korean conglomerates) never forms a
        quarterly window and lands here. When a fresh FY interest line exists we
        can still measure honestly: ``FY EBIT / FY InterestExpense`` for the
        *same* fiscal year -- a coherent annual ratio, never a mix of a
        trailing-twelve-month EBIT with an annual interest figure.

        Only ever turns what would have been NA into a measured ratio (the
        caller gates this behind material/absent debt evidence), so it cannot
        downgrade a debt-free cap. Returns ``None`` when there is no fresh
        FY interest, no aligned FY EBIT for that year, or the annual EBIT is
        non-positive (a levered loss-maker stays NA) -- letting the caller emit
        its branch-specific NA reason.
        """

        # Direct rows precede derived fallback rows in ``interest_records``, and
        # ``filter_unique_fy`` keeps the first fact per end_date, so a direct FY
        # interest line always wins its year over the derived one.
        fy_interest = filter_unique_fy(interest_records)
        fy_ebit = filter_unique_fy(ebit_records)

        # The ratio needs both legs on the same fiscal year; take the latest
        # such year whose FY interest is fresh within the annual window.
        aligned_years = [
            end_date
            for end_date, record in fy_interest.items()
            if end_date in fy_ebit
            and is_recent_fact(record, max_age_days=MAX_FY_FACT_AGE_DAYS)
        ]
        if not aligned_years:
            return None
        as_of = max(aligned_years)

        interest_record = fy_interest[as_of]
        interest_money = self._ttm_money(
            [interest_record], interest_record.concept, ebit.target_currency, listing_id
        )
        if interest_money.amount <= 0:
            # A non-positive annual interest line is "no measurable interest",
            # not a coverage reading -- defer to the caller's cap/NA decision.
            return None

        ebit_money = self._ttm_money(
            [fy_ebit[as_of]], EBIT_CONCEPTS[0], ebit.target_currency, listing_id
        )
        if ebit_money.amount <= 0:
            return None

        ratio = ebit_money / interest_money
        return MetricResult.ratio(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
        )

    def _emit_cap(self, listing_id: int, ebit: _FreshTTMEbit) -> MetricResult:
        # INFO text kept byte-identical: the console-noise regression pins it.
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
            as_of=ebit.as_of,
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


__all__ = [
    "CAP_MAX_DEBT_TO_EBIT",
    "INTEREST_COVERAGE_CAP",
    "InterestCoverageMetric",
]
