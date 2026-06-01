"""Data-transfer objects, value records, and type aliases for persistence.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Dict,
    Literal,
    NamedTuple,
    Optional,
    Tuple,
)

from pyvalue.currency import MetricUnitKind


@dataclass(frozen=True)
class Security:
    """Canonical security identity."""

    security_id: int
    canonical_ticker: str
    canonical_exchange_code: str
    canonical_symbol: str
    entity_name: Optional[str] = None
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class Exchange:
    """Canonical exchange identity."""

    exchange_id: int
    exchange_code: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def code(self) -> str:
        return self.exchange_code


@dataclass(frozen=True)
class Provider:
    """Persisted provider registry entry."""

    provider_id: int
    provider_code: str
    display_name: str
    description: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class ExchangeProvider:
    """Persisted provider-supported exchange metadata."""

    provider: str
    provider_exchange_code: str
    exchange_id: int
    exchange_code: str
    name: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    operating_mic: Optional[str] = None
    country_iso2: Optional[str] = None
    country_iso3: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def code(self) -> str:
        return self.provider_exchange_code

    @property
    def canonical_exchange_code(self) -> str:
        return self.exchange_code


@dataclass(frozen=True)
class SupportedTicker:
    """Persisted provider-supported ticker metadata."""

    provider: str
    provider_exchange_code: str
    provider_symbol: str
    provider_ticker: str
    security_id: int
    listing_exchange: Optional[str] = None
    security_name: Optional[str] = None
    security_type: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    isin: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def exchange_code(self) -> str:
        return self.provider_exchange_code

    @property
    def symbol(self) -> str:
        return self.provider_symbol

    @property
    def code(self) -> str:
        return self.provider_ticker


@dataclass(frozen=True)
class IngestProgressSummary:
    """Aggregate ingest progress for a supported-ticker scope."""

    total_supported: int
    stored: int
    missing: int
    stale: int
    blocked: int
    error_rows: int


@dataclass(frozen=True)
class IngestProgressExchange:
    """Per-exchange ingest progress for a supported-ticker scope."""

    exchange_code: str
    total_supported: int
    stored: int
    missing: int
    stale: int
    blocked: int
    error_rows: int


@dataclass(frozen=True)
class IngestProgressFailure:
    """Recent ingest failure details for reporting."""

    symbol: str
    exchange_code: str
    last_status: Optional[str] = None
    last_error: Optional[str] = None
    next_eligible_at: Optional[str] = None
    attempts: int = 0


@dataclass(frozen=True)
class FactRecord:
    """Normalized financial fact ready for storage."""

    symbol: str
    cik: Optional[str] = None
    concept: str = ""
    # fiscal_period defaults to ``"INSTANT"`` (matches the SEC company
    # facts convention for point-in-time values) rather than ``None`` so
    # the column can be NOT NULL at the schema level (migration 065).
    fiscal_period: str = "INSTANT"
    end_date: str = ""
    # ``unit_kind`` classifies the fact (monetary / per_share / count / ...);
    # the ISO currency lives in ``currency`` alone. The two are coupled at the
    # schema level (migration 071): monetary/per_share rows carry a currency,
    # every other kind carries NULL. ``currency`` is always a *major* code —
    # subunits (GBX/ZAC/ILA) are collapsed during normalization and never reach
    # a stored fact.
    unit_kind: MetricUnitKind = "other"
    value: float = 0.0
    accn: Optional[str] = None
    filed: Optional[str] = None
    frame: Optional[str] = None
    start_date: Optional[str] = None
    accounting_standard: Optional[str] = None
    currency: Optional[str] = None


@dataclass(frozen=True)
class MarketSnapshotRecord:
    """Stored latest market-data row keyed to a canonical security."""

    security_id: int
    symbol: str
    as_of: str
    price: float
    volume: Optional[int] = None
    currency: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class FinancialFactsRefreshStateRecord:
    """Latest financial-facts refresh watermark for one symbol."""

    symbol: str
    refreshed_at: str


@dataclass(frozen=True)
class MetricComputeStatusRecord:
    """Latest persisted metric-computation attempt for one symbol/metric."""

    symbol: str
    metric_id: str
    status: Literal["success", "failure"]
    attempted_at: str
    reason_code: Optional[str] = None
    reason_detail: Optional[str] = None
    value_as_of: Optional[str] = None
    facts_refreshed_at: Optional[str] = None
    market_data_as_of: Optional[str] = None
    market_data_updated_at: Optional[str] = None


StoredFactRow = Tuple[
    Optional[str],
    str,
    Optional[str],
    str,
    str,
    float,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]


class MetricRecord(NamedTuple):
    """Stored metric value with explicit unit metadata."""

    value: float
    as_of: str
    unit_kind: MetricUnitKind
    currency: Optional[str]
    unit_label: Optional[str]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, tuple) and len(other) == 2:
            return (self.value, self.as_of) == other
        return tuple.__eq__(self, other)


StoredMetricRow = Tuple[
    str,
    str,
    float,
    str,
    MetricUnitKind,
    Optional[str],
    Optional[str],
]


@dataclass(frozen=True)
class FXRateRecord:
    """Persisted direct FX rate observation."""

    provider: str
    rate_date: str
    base_currency: str
    quote_currency: str
    rate: float
    fetched_at: str
    source_kind: str
    meta_json: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class FXSupportedPairRecord:
    """Persisted FX catalog entry for one provider symbol."""

    provider: str
    symbol: str
    canonical_symbol: str
    base_currency: Optional[str]
    quote_currency: Optional[str]
    name: Optional[str]
    is_alias: bool
    is_refreshable: bool
    last_seen_at: Optional[str] = None


@dataclass(frozen=True)
class FXRefreshStateRecord:
    """Persisted refresh coverage metadata for one canonical FX symbol."""

    provider: str
    canonical_symbol: str
    min_rate_date: Optional[str]
    max_rate_date: Optional[str]
    full_history_backfilled: bool
    last_fetched_at: Optional[str]
    last_status: Optional[str]
    last_error: Optional[str]
    attempts: int


@dataclass(frozen=True)
class FundamentalsUpdate:
    """Raw fundamentals payload prepared for batch persistence."""

    security_id: int
    provider_symbol: str
    provider_exchange_code: Optional[str]
    listing_currency: Optional[str]
    data: str
    payload_hash: str
    last_fetched_at: str


@dataclass(frozen=True)
class SecurityListingStatusRecord:
    """Primary-vs-secondary listing classification for one canonical listing."""

    security_id: int
    source_provider: str
    provider_symbol: str
    raw_fetched_at: str
    is_primary_listing: bool
    primary_provider_symbol: Optional[str]
    classification_basis: Literal[
        "matched_primary_ticker",
        "different_primary_ticker",
        "missing_primary_ticker",
    ]
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class SecurityMetadataCandidate:
    """Canonical metadata extracted from stored raw fundamentals."""

    entity_name: Optional[str] = None
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    def to_update_fields(self) -> Dict[str, str]:
        """Return only metadata fields that should overwrite canonicals."""

        update_fields: Dict[str, str] = {}
        if self.entity_name is not None:
            update_fields["entity_name"] = self.entity_name
        if self.description is not None:
            update_fields["description"] = self.description
        if self.sector is not None:
            update_fields["sector"] = self.sector
        if self.industry is not None:
            update_fields["industry"] = self.industry
        return update_fields


@dataclass(frozen=True)
class SecurityMetadataUpdate:
    """Canonical security metadata prepared for batched persistence."""

    security_id: int
    entity_name: Optional[str] = None
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


@dataclass(frozen=True)
class FundamentalsNormalizationCandidate:
    """Normalization freshness inputs for one stored raw fundamentals payload."""

    provider_symbol: str
    security_id: int
    raw_payload_hash: str
    normalized_payload_hash: Optional[str] = None
    normalized_at: Optional[str] = None
    current_source_provider: Optional[str] = None


@dataclass(frozen=True)
class SupportedTickerRefreshResult:
    """Outcome of refreshing one provider/exchange supported-ticker slice.

    ``inserted`` counts the listings actually catalogued. ``skipped_no_currency``
    lists the provider tickers dropped because the payload carried no currency:
    ``listing.currency`` is NOT NULL with no fallback, so a currency-less entry
    cannot be modelled. It is surfaced to the operator so the underlying data
    issue can be chased with the provider.
    """

    inserted: int
    skipped_no_currency: Tuple[str, ...]
