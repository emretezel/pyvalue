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


def _subset_ids_by_symbol(
    ids_by_symbol: Optional[Mapping[str, int]],
    symbols: Sequence[str],
) -> Optional[Mapping[str, int]]:
    """Narrow a scope-wide listing-id map to ``symbols`` for a downstream read.

    ``states_many`` threads one scope-resolved id map to four repositories, but
    the facts-refresh and market-snapshot reads only cover the subset of symbols
    whose metrics use those inputs. Those repos key their query off the supplied
    map directly, so we narrow it here to avoid over-fetching the whole scope.
    Returns ``None`` (the resolve-internally signal) when no map was supplied.
    """
    if ids_by_symbol is None:
        return None
    return {
        symbol: ids_by_symbol[symbol] for symbol in symbols if symbol in ids_by_symbol
    }


class _CachedRegionFactsRepository(RegionFactsRepository):
    """Serve one symbol's facts from memory while preserving the repo interface."""

    def __init__(
        self,
        repo: FinancialFactsRepository,
        symbol: str,
        records: Sequence[FactRecord],
    ) -> None:
        super().__init__(repo)
        self._symbol = symbol.strip().upper()
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
        symbol: str,
        concept: str,
    ) -> Optional[FactRecord]:
        if symbol.strip().upper() != self._symbol:
            return super().latest_fact(symbol, concept)
        return self._latest_by_concept.get(concept)

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        if symbol.strip().upper() != self._symbol:
            return super().facts_for_concept(
                symbol,
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
                    "preloaded fact cache miss: symbol=%s concept=%s — "
                    "metric may have under-declared required_concepts",
                    self._symbol,
                    concept,
                )
        else:
            records = self._facts_by_concept_period.get((concept, fiscal_period), ())

        selected = list(records)
        if limit is not None:
            selected = selected[:limit]
        return selected

    def ticker_currency(self, symbol: str) -> Optional[str]:
        symbol_upper = symbol.strip().upper()
        if symbol_upper != self._symbol:
            resolver = getattr(self._repo, "ticker_currency", None)
            if callable(resolver):
                return resolver(symbol)
            return None
        if not self._ticker_currency_loaded:
            resolver = getattr(self._repo, "ticker_currency", None)
            self._ticker_currency = resolver(symbol) if callable(resolver) else None
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
    """Serve stored metric values from memory for a fixed symbol scope."""

    def __init__(
        self,
        db_path: Union[str, Path],
        metric_rows_by_symbol: Mapping[str, Mapping[str, MetricRecord]],
    ) -> None:
        super().__init__(db_path)
        self._metric_rows_by_symbol = {
            symbol.strip().upper(): dict(metric_rows)
            for symbol, metric_rows in metric_rows_by_symbol.items()
        }

    def fetch(self, symbol: str, metric_id: str) -> Optional[MetricRecord]:
        return self._metric_rows_by_symbol.get(symbol.strip().upper(), {}).get(
            metric_id
        )


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

    def state(self, symbol: str, metric_id: str) -> _MetricAvailabilityState:
        symbol_upper = symbol.strip().upper()
        record = self._raw_metrics_repo.fetch(symbol_upper, metric_id)
        status_record = self._status_repo.fetch(symbol_upper, metric_id)
        facts_refresh_record = None
        market_snapshot_record = None
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is not None and getattr(metric_cls, "uses_financial_facts", True):
            facts_refresh_record = self._facts_refresh_repo.fetch(symbol_upper)
        if metric_cls is not None and getattr(metric_cls, "uses_market_data", False):
            market_snapshot_record = self._market_repo.latest_snapshot_record(
                symbol_upper
            )
        return _build_metric_availability_state(
            metric_id,
            record,
            status_record,
            facts_refresh_record,
            market_snapshot_record,
        )

    def states_many(
        self,
        symbols: Sequence[str],
        metric_ids: Sequence[str],
        *,
        chunk_size: int = 500,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
    ) -> Dict[str, Dict[str, _MetricAvailabilityState]]:
        normalized_symbols = [symbol.strip().upper() for symbol in symbols if symbol]
        requested_metric_ids = [
            metric_id.strip() for metric_id in metric_ids if str(metric_id).strip()
        ]
        if not normalized_symbols or not requested_metric_ids:
            return {}

        # A supplied map (run-screen / report-* resolved its scope to listing ids)
        # is threaded to every underlying read so none of them re-resolves symbols.
        raw_rows = self._raw_metrics_repo.fetch_many_for_symbols(
            normalized_symbols,
            requested_metric_ids,
            chunk_size=chunk_size,
            security_ids_by_symbol=security_ids_by_symbol,
        )
        status_rows = self._status_repo.fetch_many_for_symbols(
            normalized_symbols,
            requested_metric_ids,
            chunk_size=chunk_size,
            security_ids_by_symbol=security_ids_by_symbol,
        )
        facts_refresh_symbols = sorted(
            {
                symbol
                for symbol, per_symbol_statuses in status_rows.items()
                for metric_id in per_symbol_statuses.keys()
                if getattr(REGISTRY.get(metric_id), "uses_financial_facts", True)
            }
        )
        facts_refresh_rows = (
            self._facts_refresh_repo.fetch_many_for_symbols(
                facts_refresh_symbols,
                chunk_size=chunk_size,
                security_ids_by_symbol=_subset_ids_by_symbol(
                    security_ids_by_symbol, facts_refresh_symbols
                ),
            )
            if facts_refresh_symbols
            else {}
        )
        market_snapshot_symbols = sorted(
            {
                symbol
                for symbol, per_symbol_statuses in status_rows.items()
                for metric_id in per_symbol_statuses.keys()
                if getattr(REGISTRY.get(metric_id), "uses_market_data", False)
            }
        )
        market_snapshot_rows = (
            self._market_repo.latest_snapshots_many(
                market_snapshot_symbols,
                chunk_size=chunk_size,
                security_ids_by_symbol=_subset_ids_by_symbol(
                    security_ids_by_symbol, market_snapshot_symbols
                ),
            )
            if market_snapshot_symbols
            else {}
        )

        states: Dict[str, Dict[str, _MetricAvailabilityState]] = {}
        for symbol_upper in normalized_symbols:
            per_symbol_states: Dict[str, _MetricAvailabilityState] = {}
            symbol_metric_rows = raw_rows.get(symbol_upper, {})
            symbol_status_rows = status_rows.get(symbol_upper, {})
            facts_refresh_record = facts_refresh_rows.get(symbol_upper)
            market_snapshot_record = market_snapshot_rows.get(symbol_upper)
            for metric_id in requested_metric_ids:
                per_symbol_states[metric_id] = _build_metric_availability_state(
                    metric_id,
                    symbol_metric_rows.get(metric_id),
                    symbol_status_rows.get(metric_id),
                    facts_refresh_record,
                    market_snapshot_record,
                )
            states[symbol_upper] = per_symbol_states
        return states

    def fetch(self, symbol: str, metric_id: str) -> Optional[MetricRecord]:
        return self.state(symbol, metric_id).record

    def fetch_many_for_symbols(
        self,
        symbols: Sequence[str],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
        *,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
    ) -> Dict[str, Dict[str, MetricRecord]]:
        states = self.states_many(
            symbols,
            metric_ids,
            chunk_size=chunk_size,
            security_ids_by_symbol=security_ids_by_symbol,
        )
        rows_by_symbol: Dict[str, Dict[str, MetricRecord]] = {}
        for symbol, per_symbol_states in states.items():
            for metric_id, state in per_symbol_states.items():
                if state.record is None:
                    continue
                rows_by_symbol.setdefault(symbol, {})[metric_id] = state.record
        return rows_by_symbol


class _CachedMarketDataRepository:
    """Serve one symbol's latest market snapshot from memory."""

    def __init__(
        self,
        repo: MarketDataRepository,
        symbol: str,
        *,
        snapshot: Optional[PriceData] = None,
        snapshot_loaded: bool = False,
    ) -> None:
        self._repo = repo
        self._symbol = symbol.strip().upper()
        self._snapshot_loaded = snapshot_loaded
        self._snapshot: Optional[PriceData] = snapshot
        self._ticker_currency_loaded = False
        self._ticker_currency: Optional[str] = None

    def _load_snapshot(self) -> None:
        if self._snapshot_loaded:
            return
        self._snapshot = self._repo.latest_snapshot(self._symbol)
        self._snapshot_loaded = True

    def latest_snapshot(self, symbol: str) -> Optional[PriceData]:
        if symbol.strip().upper() != self._symbol:
            return self._repo.latest_snapshot(symbol)
        self._load_snapshot()
        return self._snapshot

    def latest_price(self, symbol: str) -> Optional[Tuple[str, float]]:
        snapshot = self.latest_snapshot(symbol)
        if snapshot is None:
            return None
        return snapshot.as_of, snapshot.price

    def ticker_currency(self, symbol: str) -> Optional[str]:
        symbol_upper = symbol.strip().upper()
        if symbol_upper != self._symbol:
            resolver = getattr(self._repo, "ticker_currency", None)
            if callable(resolver):
                return resolver(symbol)
            return None
        if not self._ticker_currency_loaded:
            resolver = getattr(self._repo, "ticker_currency", None)
            self._ticker_currency = resolver(symbol) if callable(resolver) else None
            self._ticker_currency_loaded = True
        return self._ticker_currency

    def __getattr__(self, name: str) -> Any:
        # Transparent proxy to the wrapped repo for anything not overridden
        # above; ``Any`` is the right type for a dynamic forwarder.
        return getattr(self._repo, name)
