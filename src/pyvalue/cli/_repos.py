"""Schema-ready and caching repository wrappers used across CLI commands.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.marketdata import PriceData
from pyvalue.metrics import REGISTRY
from pyvalue.facts import RegionFactsRepository
from pyvalue.persistence.storage import (
    FXRatesRepository,
    FinancialFactsRepository,
    FinancialFactsRefreshStateRecord,
    FinancialFactsRefreshStateRepository,
    FactRecord,
    MarketDataRepository,
    MarketSnapshotRecord,
    MetricComputeStatusRecord,
    MetricComputeStatusRepository,
    MetricRecord,
    MetricsRepository,
    SecurityRepository,
    SupportedTickerRepository,
)

from ._common import (
    LOGGER,
    _MetricAvailabilityState,
)


class _CachedRegionFactsRepository(RegionFactsRepository):
    """Serve one listing's facts from memory while preserving the repo interface."""

    def __init__(
        self,
        repo: FinancialFactsRepository,
        listing_id: int,
        records: Sequence[FactRecord],
    ) -> None:
        super().__init__(repo)
        self._listing_id = int(listing_id)
        self._ticker_currency_loaded = False
        self._ticker_currency: Optional[str] = None
        self._latest_by_concept: Dict[str, FactRecord] = {}
        self._facts_by_concept: Dict[str, Tuple[FactRecord, ...]] = {}
        facts_by_concept: Dict[str, List[FactRecord]] = {}
        facts_by_concept_period: Dict[Tuple[str, str], List[FactRecord]] = {}

        for record in records:
            facts_by_concept.setdefault(record.concept, []).append(record)
            if record.fiscal_period:
                facts_by_concept_period.setdefault(
                    (record.concept, record.fiscal_period), []
                ).append(record)
            self._latest_by_concept.setdefault(record.concept, record)

        self._facts_by_concept = {
            concept: tuple(concept_records)
            for concept, concept_records in facts_by_concept.items()
        }
        self._facts_by_concept_period = {
            key: tuple(concept_records)
            for key, concept_records in facts_by_concept_period.items()
        }

    def latest_fact(
        self,
        listing_id: int,
        concept: str,
    ) -> Optional[FactRecord]:
        if int(listing_id) != self._listing_id:
            return super().latest_fact(listing_id, concept)
        return self._latest_by_concept.get(concept)

    def facts_for_concept(
        self,
        listing_id: int,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        if int(listing_id) != self._listing_id:
            return super().facts_for_concept(
                listing_id,
                concept,
                fiscal_period=fiscal_period,
                limit=limit,
            )

        if fiscal_period is None:
            records = self._facts_by_concept.get(concept, ())
            # Surface a metric that asked for a concept the preload didn't
            # fetch — typically the metric under-declared its
            # ``required_concepts`` and would silently degrade to N+1 reads
            # against the live DB once we re-enable the concept filter on
            # the preload. DEBUG so production stays quiet, but tests can
            # opt in by setting the logger level.
            if not records and concept not in self._facts_by_concept:
                LOGGER.debug(
                    "preloaded fact cache miss: listing_id=%s concept=%s — "
                    "metric may have under-declared required_concepts",
                    self._listing_id,
                    concept,
                )
        else:
            records = self._facts_by_concept_period.get((concept, fiscal_period), ())

        selected = list(records)
        if limit is not None:
            selected = selected[:limit]
        return selected

    def ticker_currency_by_id(self, listing_id: int) -> Optional[str]:
        if int(listing_id) != self._listing_id:
            resolver = getattr(self._repo, "ticker_currency_by_id", None)
            if callable(resolver):
                return resolver(listing_id)
            return None
        if not self._ticker_currency_loaded:
            resolver = getattr(self._repo, "ticker_currency_by_id", None)
            self._ticker_currency = resolver(listing_id) if callable(resolver) else None
            self._ticker_currency_loaded = True
        return self._ticker_currency


class _SchemaReadySecurityRepository(SecurityRepository):
    """Read-only security repository for metric workers on an initialized DB."""

    def initialize_schema(self) -> None:
        return


class _SchemaReadyFXRatesRepository(FXRatesRepository):
    """FX rates repository that skips schema init in normalization workers."""

    def initialize_schema(self) -> None:
        return


class _SchemaReadySupportedTickerRepository(SupportedTickerRepository):
    """Supported-ticker repository that skips schema init in normalization workers."""

    def initialize_schema(self) -> None:
        return


class _SchemaReadyFinancialFactsRepository(FinancialFactsRepository):
    """Read-only facts repository that skips schema work in metric workers."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyMarketDataRepository(MarketDataRepository):
    """Read-only market-data repository that skips schema work in workers."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyMetricsRepository(MetricsRepository):
    """Metrics writer that assumes the schema is already initialized."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyMetricComputeStatusRepository(MetricComputeStatusRepository):
    """Metric-status repository that assumes the schema is already initialized."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyFinancialFactsRefreshStateRepository(
    FinancialFactsRefreshStateRepository
):
    """Facts-refresh-state repository that assumes the schema is ready."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _PreloadedMetricsRepository(_SchemaReadyMetricsRepository):
    """Serve stored metric values from memory for a fixed listing scope."""

    def __init__(
        self,
        db_path: Union[str, Path],
        metric_rows_by_id: Mapping[int, Mapping[str, MetricRecord]],
    ) -> None:
        super().__init__(db_path)
        self._metric_rows_by_id = {
            int(listing_id): dict(metric_rows)
            for listing_id, metric_rows in metric_rows_by_id.items()
        }

    def fetch_by_id(self, listing_id: int, metric_id: str) -> Optional[MetricRecord]:
        return self._metric_rows_by_id.get(int(listing_id), {}).get(metric_id)


def _metric_status_current_facts_refresh(
    record: Optional[FinancialFactsRefreshStateRecord],
) -> Optional[str]:
    return record.refreshed_at if record is not None else None


def _metric_status_current_market_watermark(
    record: Optional[MarketSnapshotRecord],
) -> Tuple[Optional[str], Optional[str]]:
    if record is None:
        return None, None
    return record.as_of, record.updated_at


def _build_metric_availability_state(
    metric_id: str,
    record: Optional[MetricRecord],
    status_record: Optional[MetricComputeStatusRecord],
    facts_refresh_record: Optional[FinancialFactsRefreshStateRecord],
    market_snapshot_record: Optional[MarketSnapshotRecord],
) -> _MetricAvailabilityState:
    if status_record is None:
        return _MetricAvailabilityState(
            metric_id=metric_id,
            record=record,
            status_record=None,
            stale=False,
        )

    metric_cls = REGISTRY.get(metric_id)
    uses_financial_facts = bool(metric_cls) and getattr(
        metric_cls, "uses_financial_facts", True
    )
    uses_market_data = bool(metric_cls) and getattr(
        metric_cls, "uses_market_data", False
    )
    stale = False
    current_facts_refreshed_at = _metric_status_current_facts_refresh(
        facts_refresh_record
    )
    current_market_data_as_of, current_market_data_updated_at = (
        _metric_status_current_market_watermark(market_snapshot_record)
    )

    if uses_financial_facts and (
        status_record.facts_refreshed_at != current_facts_refreshed_at
    ):
        stale = True
    if uses_market_data and (
        status_record.market_data_as_of != current_market_data_as_of
        or status_record.market_data_updated_at != current_market_data_updated_at
    ):
        stale = True
    if status_record.status == "success":
        if record is None:
            stale = True
        elif (
            status_record.value_as_of is not None
            and record.as_of != status_record.value_as_of
        ):
            stale = True

    if stale:
        return _MetricAvailabilityState(
            metric_id=metric_id,
            record=None,
            status_record=status_record,
            stale=True,
        )
    if status_record.status == "failure":
        return _MetricAvailabilityState(
            metric_id=metric_id,
            record=None,
            status_record=status_record,
            stale=False,
        )
    return _MetricAvailabilityState(
        metric_id=metric_id,
        record=record,
        status_record=status_record,
        stale=False,
    )


class _StatusAwareMetricsRepository(_SchemaReadyMetricsRepository):
    """Expose metric reads with persisted latest-attempt status shadowing."""

    def __init__(
        self,
        db_path: Union[str, Path],
        *,
        raw_metrics_repo: Optional[MetricsRepository] = None,
        status_repo: Optional[MetricComputeStatusRepository] = None,
        facts_refresh_repo: Optional[FinancialFactsRefreshStateRepository] = None,
        market_repo: Optional[MarketDataRepository] = None,
    ) -> None:
        super().__init__(db_path)
        self._raw_metrics_repo = raw_metrics_repo or _SchemaReadyMetricsRepository(
            db_path
        )
        self._status_repo = status_repo or _SchemaReadyMetricComputeStatusRepository(
            db_path
        )
        self._facts_refresh_repo = (
            facts_refresh_repo
            or _SchemaReadyFinancialFactsRefreshStateRepository(db_path)
        )
        self._market_repo = market_repo or _SchemaReadyMarketDataRepository(db_path)

    def state_by_id(self, listing_id: int, metric_id: str) -> _MetricAvailabilityState:
        """Natural-identity counterpart of :meth:`state`.

        Reads the raw metric, the latest-attempt status, and (only when the
        metric declares it uses them) the facts-refresh / market watermarks via
        the ``*_by_id`` readers, so no symbol resolution occurs.
        """

        record = self._raw_metrics_repo.fetch_by_id(listing_id, metric_id)
        status_record = self._status_repo.fetch_by_id(listing_id, metric_id)
        facts_refresh_record = None
        market_snapshot_record = None
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is not None and getattr(metric_cls, "uses_financial_facts", True):
            facts_refresh_record = self._facts_refresh_repo.fetch_by_id(listing_id)
        if metric_cls is not None and getattr(metric_cls, "uses_market_data", False):
            market_snapshot_record = self._market_repo.latest_snapshot_record_by_id(
                listing_id
            )
        return _build_metric_availability_state(
            metric_id,
            record,
            status_record,
            facts_refresh_record,
            market_snapshot_record,
        )

    def states_many_by_ids(
        self,
        listing_ids: Sequence[int],
        metric_ids: Sequence[str],
        *,
        chunk_size: int = 500,
    ) -> Dict[int, Dict[str, _MetricAvailabilityState]]:
        """Natural-identity counterpart of :meth:`states_many`.

        Facts-refresh and market watermarks are fetched only for listings that
        have a status row (mirroring :meth:`states_many`): when a metric has no
        prior status, :func:`_build_metric_availability_state` ignores those
        watermarks, so reading them would be wasted work.
        """

        normalized_ids = [int(listing_id) for listing_id in listing_ids]
        requested_metric_ids = [
            metric_id.strip() for metric_id in metric_ids if str(metric_id).strip()
        ]
        if not normalized_ids or not requested_metric_ids:
            return {}

        raw_rows = self._raw_metrics_repo.fetch_many_by_ids(
            normalized_ids, requested_metric_ids, chunk_size=chunk_size
        )
        status_rows = self._status_repo.fetch_many_by_ids(
            normalized_ids, requested_metric_ids, chunk_size=chunk_size
        )
        facts_refresh_ids = sorted(
            {
                listing_id
                for listing_id, per_listing_statuses in status_rows.items()
                for metric_id in per_listing_statuses.keys()
                if getattr(REGISTRY.get(metric_id), "uses_financial_facts", True)
            }
        )
        facts_refresh_rows = (
            self._facts_refresh_repo.fetch_many_by_ids(
                facts_refresh_ids, chunk_size=chunk_size
            )
            if facts_refresh_ids
            else {}
        )
        market_snapshot_ids = sorted(
            {
                listing_id
                for listing_id, per_listing_statuses in status_rows.items()
                for metric_id in per_listing_statuses.keys()
                if getattr(REGISTRY.get(metric_id), "uses_market_data", False)
            }
        )
        market_snapshot_rows = (
            self._market_repo.latest_snapshots_many_by_ids(
                market_snapshot_ids, chunk_size=chunk_size
            )
            if market_snapshot_ids
            else {}
        )

        states: Dict[int, Dict[str, _MetricAvailabilityState]] = {}
        for listing_id in normalized_ids:
            per_listing_states: Dict[str, _MetricAvailabilityState] = {}
            listing_metric_rows = raw_rows.get(listing_id, {})
            listing_status_rows = status_rows.get(listing_id, {})
            facts_refresh_record = facts_refresh_rows.get(listing_id)
            market_snapshot_record = market_snapshot_rows.get(listing_id)
            for metric_id in requested_metric_ids:
                per_listing_states[metric_id] = _build_metric_availability_state(
                    metric_id,
                    listing_metric_rows.get(metric_id),
                    listing_status_rows.get(metric_id),
                    facts_refresh_record,
                    market_snapshot_record,
                )
            states[listing_id] = per_listing_states
        return states

    def fetch_by_id(self, listing_id: int, metric_id: str) -> Optional[MetricRecord]:
        return self.state_by_id(listing_id, metric_id).record

    def fetch_many_by_ids(
        self,
        listing_ids: Sequence[int],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[int, Dict[str, MetricRecord]]:
        states = self.states_many_by_ids(
            listing_ids,
            metric_ids,
            chunk_size=chunk_size,
        )
        rows_by_id: Dict[int, Dict[str, MetricRecord]] = {}
        for listing_id, per_listing_states in states.items():
            for metric_id, state in per_listing_states.items():
                if state.record is None:
                    continue
                rows_by_id.setdefault(listing_id, {})[metric_id] = state.record
        return rows_by_id


class _CachedMarketDataRepository:
    """Serve one listing's latest market snapshot from memory."""

    def __init__(
        self,
        repo: MarketDataRepository,
        listing_id: int,
        *,
        snapshot: Optional[PriceData] = None,
        snapshot_loaded: bool = False,
    ) -> None:
        self._repo = repo
        self._listing_id = int(listing_id)
        self._snapshot_loaded = snapshot_loaded
        self._snapshot: Optional[PriceData] = snapshot
        self._ticker_currency_loaded = False
        self._ticker_currency: Optional[str] = None

    def _load_snapshot(self) -> None:
        if self._snapshot_loaded:
            return
        self._snapshot = self._repo.latest_snapshot_by_id(self._listing_id)
        self._snapshot_loaded = True

    def latest_snapshot_by_id(self, listing_id: int) -> Optional[PriceData]:
        if int(listing_id) != self._listing_id:
            return self._repo.latest_snapshot_by_id(listing_id)
        self._load_snapshot()
        return self._snapshot

    def latest_price_by_id(self, listing_id: int) -> Optional[Tuple[str, float]]:
        snapshot = self.latest_snapshot_by_id(listing_id)
        if snapshot is None:
            return None
        return snapshot.as_of, snapshot.price

    def ticker_currency_by_id(self, listing_id: int) -> Optional[str]:
        if int(listing_id) != self._listing_id:
            resolver = getattr(self._repo, "ticker_currency_by_id", None)
            if callable(resolver):
                return resolver(listing_id)
            return None
        if not self._ticker_currency_loaded:
            resolver = getattr(self._repo, "ticker_currency_by_id", None)
            self._ticker_currency = resolver(listing_id) if callable(resolver) else None
            self._ticker_currency_loaded = True
        return self._ticker_currency

    def __getattr__(self, name: str) -> Any:
        # Transparent proxy to the wrapped repo for anything not overridden
        # above; ``Any`` is the right type for a dynamic forwarder.
        return getattr(self._repo, name)
