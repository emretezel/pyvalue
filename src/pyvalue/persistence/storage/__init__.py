"""Facade re-exporting the persistence storage public API.

This package was split from a single ``storage.py`` module. The submodules
hold the implementation; this ``__init__`` re-exports the full public surface
so that every existing ``from pyvalue.persistence.storage import X`` import
keeps working unchanged.

Author: Emre Tezel
"""

from __future__ import annotations

from .base import (
    canonical_json_dumps,
    fundamentals_payload_hash,
)

# Several names below are re-exported for backwards compatibility but are not
# listed in ``__all__`` (which is kept byte-identical to the original module).
# Those lines carry a per-line F401 suppression marking them as intentional
# re-exports so ruff does not flag them as unused.
from .records import (
    Exchange,
    ExchangeProvider,
    FXRateRecord,
    FXRefreshStateRecord,  # noqa: F401
    FXSupportedPairRecord,  # noqa: F401
    FactRecord,
    FinancialFactsRefreshStateRecord,
    FundamentalsUpdate,  # noqa: F401
    IdKeyedStoredMetricRow,  # noqa: F401
    IngestProgressExchange,
    IngestProgressFailure,
    IngestProgressSummary,
    MarketSnapshotRecord,
    MetricComputeStatusRecord,
    MetricRecord,
    MetricStatusAggregate,
    NormalizationUnit,  # noqa: F401
    Provider,  # noqa: F401
    Security,
    SecurityListingStatusRecord,  # noqa: F401
    SecurityMetadataCandidate,
    SecurityMetadataUpdate,
    StoredFactRow,  # noqa: F401
    StoredMetricRow,  # noqa: F401
    SupportedTicker,
    SupportedTickerRefreshResult,
)
from .entities import (
    ExchangeProviderRepository,
    ExchangeRepository,
    ProviderRepository,  # noqa: F401
    SecurityRepository,
)
from .supported_tickers import SupportedTickerRepository
from .fundamentals import (
    FundamentalsNormalizationStateRepository,  # noqa: F401
    FundamentalsRepository,
)
from .listing_status import SecurityListingStatusRepository  # noqa: F401
from .fetch_state import (
    FundamentalsFetchStateRepository,
    MarketDataFetchStateRepository,
)
from .financial_facts import (
    FinancialFactsRefreshStateRepository,
    FinancialFactsRepository,
)
from .fx import (
    FXRatesRepository,
    FXRefreshStateRepository,  # noqa: F401
    FXSupportedPairsRepository,  # noqa: F401
)
from .metrics_market import (
    MarketDataRepository,
    MetricComputeStatusRepository,
    MetricsRepository,
    MetricsWriteSession,
)

# Backwards-compatible aliases (verbatim from the original module tail). These
# preserve older names that callers still import.
ListingRepository = SecurityRepository
ProviderExchangeRepository = ExchangeProviderRepository
ProviderListingRepository = SupportedTickerRepository
ProviderListing = SupportedTicker

__all__ = [
    "Exchange",
    "ExchangeProvider",
    "ExchangeProviderRepository",
    "ExchangeRepository",
    "FXRateRecord",
    "FXRatesRepository",
    "Security",
    "SecurityMetadataCandidate",
    "SecurityMetadataUpdate",
    "SecurityRepository",
    "ListingRepository",
    "FundamentalsRepository",
    "IngestProgressSummary",
    "IngestProgressExchange",
    "IngestProgressFailure",
    "SupportedTicker",
    "SupportedTickerRefreshResult",
    "SupportedTickerRepository",
    "ProviderExchangeRepository",
    "ProviderListing",
    "ProviderListingRepository",
    "FundamentalsFetchStateRepository",
    "MarketDataFetchStateRepository",
    "FinancialFactsRepository",
    "FinancialFactsRefreshStateRecord",
    "FinancialFactsRefreshStateRepository",
    "MarketDataRepository",
    "FactRecord",
    "MarketSnapshotRecord",
    "MetricComputeStatusRecord",
    "MetricComputeStatusRepository",
    "MetricRecord",
    "MetricStatusAggregate",
    "MetricsRepository",
    "MetricsWriteSession",
    "canonical_json_dumps",
    "fundamentals_payload_hash",
]
