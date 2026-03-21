"""Metric computation interfaces and implementations.

Author: Emre Tezel
"""

from .base import Metric, MetricResult
from .working_capital import WorkingCapitalMetric
from .current_ratio import CurrentRatioMetric
from .debt_paydown_years import DebtPaydownYearsMetric, FCFToDebtMetric
from .cash_conversion import CFOToNITTMMetric, CFOToNITenYearMedianMetric
from .fundamental_consistency import (
    FCFFiveYearMedianMetric,
    FCFNegativeYearsTenYearMetric,
    NetIncomeLossYearsTenYearMetric,
)
from .accruals_ratio import AccrualsRatioMetric
from .long_term_debt import LongTermDebtMetric
from .eps_streak import EPSStreakMetric
from .eps_quarterly import EarningsPerShareTTM
from .eps_average import EPSAverageSixYearMetric
from .graham_eps_cagr import GrahamEPSCAGRMetric
from .graham_multiplier import GrahamMultiplierMetric
from .earnings_yield import EarningsYieldMetric
from .interest_coverage import InterestCoverageMetric
from .market_capitalization import MarketCapitalizationMetric
from .price_to_fcf import PriceToFCFMetric
from .roc_greenblatt import ROCGreenblattMetric
from .roe_greenblatt import ROEGreenblattMetric
from .net_debt_to_ebitda import NetDebtToEBITDAMetric
from .short_term_debt_share import ShortTermDebtShareMetric
from .invested_capital import ICMostRecentQuarterMetric, ICFYMetric, AvgICMetric
from .roic_ttm import RoicTTMMetric
from .roic_fy_series import (
    IncrementalROICFiveYearMetric,
    ROIC10YMedianMetric,
    ROICYearsAbove12PctMetric,
    ROIC10YMinMetric,
)
from .return_on_invested_capital import ReturnOnInvestedCapitalMetric
from .mcapex import MCapexFYMetric, MCapexFiveYearMetric, MCapexTTMMetric
from .nwc import (
    NWCMostRecentQuarterMetric,
    NWCFYMetric,
    DeltaNWCTTMMetric,
    DeltaNWCFYMetric,
    DeltaNWCMaintMetric,
)
from .owner_earnings_equity import (
    OwnerEarningsEquityTTMMetric,
    OwnerEarningsEquityFiveYearAverageMetric,
)
from .owner_earnings_yield import (
    OwnerEarningsYieldEquityMetric,
    OwnerEarningsYieldEquityFiveYearMetric,
    OwnerEarningsYieldEVMetric,
    OwnerEarningsYieldEVNormalizedMetric,
)
from .owner_earnings_enterprise import (
    OwnerEarningsEnterpriseTTMMetric,
    OwnerEarningsEnterpriseFiveYearAverageMetric,
    OwnerEarningsEnterpriseFiveYearMedianMetric,
    WorstOwnerEarningsEnterpriseTenYearMetric,
)
from .gross_margin_stability import GrossMarginTenYearStdMetric
from .operating_margin_stability import (
    OperatingMarginTenYearMinMetric,
    OperatingMarginTenYearStdMetric,
)
from .share_count_change import ShareCountCAGR10YMetric, Shares10YPctChangeMetric
from .buyback_yield import NetBuybackYieldMetric
from .enterprise_value_ratios import (
    EBITYieldEVMetric,
    FCFYieldEVMetric,
    EVToEBITMetric,
    EVToEBITDAMetric,
)
from .sbc_load import SBCToFCFMetric, SBCToRevenueMetric

REGISTRY = {
    WorkingCapitalMetric.id: WorkingCapitalMetric,
    CurrentRatioMetric.id: CurrentRatioMetric,
    DebtPaydownYearsMetric.id: DebtPaydownYearsMetric,
    FCFToDebtMetric.id: FCFToDebtMetric,
    CFOToNITTMMetric.id: CFOToNITTMMetric,
    CFOToNITenYearMedianMetric.id: CFOToNITenYearMedianMetric,
    FCFFiveYearMedianMetric.id: FCFFiveYearMedianMetric,
    FCFNegativeYearsTenYearMetric.id: FCFNegativeYearsTenYearMetric,
    NetIncomeLossYearsTenYearMetric.id: NetIncomeLossYearsTenYearMetric,
    AccrualsRatioMetric.id: AccrualsRatioMetric,
    LongTermDebtMetric.id: LongTermDebtMetric,
    EPSStreakMetric.id: EPSStreakMetric,
    EarningsPerShareTTM.id: EarningsPerShareTTM,
    EPSAverageSixYearMetric.id: EPSAverageSixYearMetric,
    GrahamEPSCAGRMetric.id: GrahamEPSCAGRMetric,
    GrahamMultiplierMetric.id: GrahamMultiplierMetric,
    EarningsYieldMetric.id: EarningsYieldMetric,
    InterestCoverageMetric.id: InterestCoverageMetric,
    MarketCapitalizationMetric.id: MarketCapitalizationMetric,
    PriceToFCFMetric.id: PriceToFCFMetric,
    NetDebtToEBITDAMetric.id: NetDebtToEBITDAMetric,
    ShortTermDebtShareMetric.id: ShortTermDebtShareMetric,
    ICMostRecentQuarterMetric.id: ICMostRecentQuarterMetric,
    ICFYMetric.id: ICFYMetric,
    AvgICMetric.id: AvgICMetric,
    RoicTTMMetric.id: RoicTTMMetric,
    ROIC10YMedianMetric.id: ROIC10YMedianMetric,
    ROICYearsAbove12PctMetric.id: ROICYearsAbove12PctMetric,
    ROIC10YMinMetric.id: ROIC10YMinMetric,
    IncrementalROICFiveYearMetric.id: IncrementalROICFiveYearMetric,
    GrossMarginTenYearStdMetric.id: GrossMarginTenYearStdMetric,
    OperatingMarginTenYearStdMetric.id: OperatingMarginTenYearStdMetric,
    OperatingMarginTenYearMinMetric.id: OperatingMarginTenYearMinMetric,
    ShareCountCAGR10YMetric.id: ShareCountCAGR10YMetric,
    Shares10YPctChangeMetric.id: Shares10YPctChangeMetric,
    NetBuybackYieldMetric.id: NetBuybackYieldMetric,
    EBITYieldEVMetric.id: EBITYieldEVMetric,
    FCFYieldEVMetric.id: FCFYieldEVMetric,
    EVToEBITMetric.id: EVToEBITMetric,
    EVToEBITDAMetric.id: EVToEBITDAMetric,
    SBCToRevenueMetric.id: SBCToRevenueMetric,
    SBCToFCFMetric.id: SBCToFCFMetric,
    ReturnOnInvestedCapitalMetric.id: ReturnOnInvestedCapitalMetric,
    MCapexFYMetric.id: MCapexFYMetric,
    MCapexFiveYearMetric.id: MCapexFiveYearMetric,
    MCapexTTMMetric.id: MCapexTTMMetric,
    NWCMostRecentQuarterMetric.id: NWCMostRecentQuarterMetric,
    NWCFYMetric.id: NWCFYMetric,
    DeltaNWCTTMMetric.id: DeltaNWCTTMMetric,
    DeltaNWCFYMetric.id: DeltaNWCFYMetric,
    DeltaNWCMaintMetric.id: DeltaNWCMaintMetric,
    OwnerEarningsEquityTTMMetric.id: OwnerEarningsEquityTTMMetric,
    OwnerEarningsEquityFiveYearAverageMetric.id: OwnerEarningsEquityFiveYearAverageMetric,
    OwnerEarningsYieldEquityMetric.id: OwnerEarningsYieldEquityMetric,
    OwnerEarningsYieldEquityFiveYearMetric.id: OwnerEarningsYieldEquityFiveYearMetric,
    OwnerEarningsYieldEVMetric.id: OwnerEarningsYieldEVMetric,
    OwnerEarningsYieldEVNormalizedMetric.id: OwnerEarningsYieldEVNormalizedMetric,
    OwnerEarningsEnterpriseTTMMetric.id: OwnerEarningsEnterpriseTTMMetric,
    OwnerEarningsEnterpriseFiveYearAverageMetric.id: OwnerEarningsEnterpriseFiveYearAverageMetric,
    OwnerEarningsEnterpriseFiveYearMedianMetric.id: OwnerEarningsEnterpriseFiveYearMedianMetric,
    WorstOwnerEarningsEnterpriseTenYearMetric.id: WorstOwnerEarningsEnterpriseTenYearMetric,
    ROCGreenblattMetric.id: ROCGreenblattMetric,
    ROEGreenblattMetric.id: ROEGreenblattMetric,
}

__all__ = [
    "Metric",
    "MetricResult",
    "WorkingCapitalMetric",
    "CurrentRatioMetric",
    "DebtPaydownYearsMetric",
    "FCFToDebtMetric",
    "CFOToNITTMMetric",
    "CFOToNITenYearMedianMetric",
    "FCFFiveYearMedianMetric",
    "FCFNegativeYearsTenYearMetric",
    "NetIncomeLossYearsTenYearMetric",
    "AccrualsRatioMetric",
    "LongTermDebtMetric",
    "EPSStreakMetric",
    "EarningsPerShareTTM",
    "EPSAverageSixYearMetric",
    "GrahamEPSCAGRMetric",
    "GrahamMultiplierMetric",
    "EarningsYieldMetric",
    "InterestCoverageMetric",
    "MarketCapitalizationMetric",
    "PriceToFCFMetric",
    "NetDebtToEBITDAMetric",
    "ShortTermDebtShareMetric",
    "ICMostRecentQuarterMetric",
    "ICFYMetric",
    "AvgICMetric",
    "RoicTTMMetric",
    "ROIC10YMedianMetric",
    "ROICYearsAbove12PctMetric",
    "ROIC10YMinMetric",
    "IncrementalROICFiveYearMetric",
    "GrossMarginTenYearStdMetric",
    "OperatingMarginTenYearStdMetric",
    "OperatingMarginTenYearMinMetric",
    "ShareCountCAGR10YMetric",
    "Shares10YPctChangeMetric",
    "NetBuybackYieldMetric",
    "EBITYieldEVMetric",
    "FCFYieldEVMetric",
    "EVToEBITMetric",
    "EVToEBITDAMetric",
    "SBCToRevenueMetric",
    "SBCToFCFMetric",
    "ReturnOnInvestedCapitalMetric",
    "MCapexFYMetric",
    "MCapexFiveYearMetric",
    "MCapexTTMMetric",
    "NWCMostRecentQuarterMetric",
    "NWCFYMetric",
    "DeltaNWCTTMMetric",
    "DeltaNWCFYMetric",
    "DeltaNWCMaintMetric",
    "OwnerEarningsEquityTTMMetric",
    "OwnerEarningsEquityFiveYearAverageMetric",
    "OwnerEarningsYieldEquityMetric",
    "OwnerEarningsYieldEquityFiveYearMetric",
    "OwnerEarningsYieldEVMetric",
    "OwnerEarningsYieldEVNormalizedMetric",
    "OwnerEarningsEnterpriseTTMMetric",
    "OwnerEarningsEnterpriseFiveYearAverageMetric",
    "OwnerEarningsEnterpriseFiveYearMedianMetric",
    "WorstOwnerEarningsEnterpriseTenYearMetric",
    "ROCGreenblattMetric",
    "ROEGreenblattMetric",
    "REGISTRY",
]
