"""Access to normalized financial facts.

This is the *metric-facing* read layer over :class:`FinancialFactsRepository`.
The raw DAO returns :class:`FactRecord` whose ``value`` is a bare ``float`` next
to a separate ``currency`` -- nothing stops a metric reading a monetary magnitude
as a plain number and combining two currencies silently. This module maps each
raw record into a *kind-tagged* fact:

* :class:`MonetaryFact` -- a monetary or per-share fact whose amount is a
  currency-carrying :class:`~pyvalue.money.Money`. There is deliberately **no**
  ``value`` attribute, so the magnitude is unreachable except through ``Money``
  (and thus cannot mix currencies without raising).
* :class:`ScalarFact` -- a dimensionless / count fact: a bare ``float`` with no
  currency (share counts, ratios, percentages, multiples).

The discriminant is the stored ``unit_kind`` (migration 071 couples
``monetary``/``per_share`` rows to a NOT NULL currency; every other kind carries
NULL). Metrics declare intent by *which accessor* they call -- a revenue metric
calls :meth:`monetary_facts_for_concept`, a share-count metric calls
:meth:`scalar_facts_for_concept` -- and the layer validates that intent against
the stored ``unit_kind``, raising on a real mismatch rather than silently
coercing. This replaces per-call-site ``isinstance`` narrowing.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, List, Optional, Protocol, runtime_checkable

from pyvalue.currency import MetricUnitKind, is_monetary_unit_kind
from pyvalue.money import Money
from pyvalue.persistence.storage import FactRecord

LOGGER = logging.getLogger(__name__)


@runtime_checkable
class RawFactSource(Protocol):
    """Structural type for the raw ``FactRecord`` source the layer wraps.

    Depending on this protocol (rather than the concrete
    :class:`FinancialFactsRepository`) lets in-memory fakes and the batch cache
    satisfy the layer without subclassing the SQLite DAO.
    """

    def latest_fact(self, symbol: str, concept: str) -> Optional[FactRecord]: ...

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]: ...


class FactView(Protocol):
    """Read-only metadata surface shared by raw and kind-tagged facts.

    The metric *metadata* helpers (recency, fiscal-year (FY) filtering,
    quarterly selection) only ever read these provenance fields -- never the
    amount -- so typing them against this protocol lets them accept both a raw
    :class:`~pyvalue.persistence.storage.FactRecord` (plain attributes) and a kind-tagged
    :class:`MonetaryFact`/:class:`ScalarFact` (properties) interchangeably.
    Declared as read-only properties so a plain attribute *or* a ``@property``
    satisfies it structurally.
    """

    @property
    def end_date(self) -> str: ...

    @property
    def fiscal_period(self) -> str: ...


@dataclass(frozen=True)
class _TypedFact:
    """Shared metadata for a kind-tagged fact.

    Wraps the originating :class:`FactRecord` and re-exposes the metadata fields
    metrics actually read (dates, period, provenance) as pass-throughs,
    so call sites keep using ``fact.end_date`` etc. The *amount* is intentionally
    not exposed here -- the kind-specific subclass decides how it may be read.
    """

    record: FactRecord

    @property
    def concept(self) -> str:
        return self.record.concept

    @property
    def end_date(self) -> str:
        return self.record.end_date

    @property
    def fiscal_period(self) -> str:
        return self.record.fiscal_period

    @property
    def filed(self) -> Optional[str]:
        return self.record.filed

    @property
    def symbol(self) -> str:
        return self.record.symbol

    @property
    def unit_kind(self) -> MetricUnitKind:
        return self.record.unit_kind


@dataclass(frozen=True)
class MonetaryFact(_TypedFact):
    """A monetary or per-share fact whose amount carries its currency.

    ``money`` replaces the raw record's bare-float ``value``: there is no
    ``.value`` here, so a metric cannot read the magnitude without going through
    :class:`~pyvalue.money.Money` and therefore cannot mix currencies silently.
    Per-share facts (EPS, dividends-per-share) are monetary too -- a per-share
    amount is still money-with-a-currency -- so they are ``MonetaryFact`` as well.
    """

    money: Money


@dataclass(frozen=True)
class ScalarFact(_TypedFact):
    """A dimensionless or count fact: a bare ``float`` with no currency."""

    @property
    def value(self) -> float:
        return self.record.value


def to_monetary_fact(record: FactRecord) -> Optional[MonetaryFact]:
    """Mint a :class:`MonetaryFact`, or ``None`` if no usable ``Money`` can form.

    Raises ``ValueError`` when ``record`` is not a monetary/per-share fact: that
    means a metric asked for money on a non-monetary concept (a programming
    error), which is surfaced rather than silently coerced. A monetary fact that
    cannot resolve a currency is dropped with a warning instead of crashing the
    batch -- migration 071's coupled CHECK should make that unreachable, so it
    signals a normalization bug if it ever fires.
    """

    if not is_monetary_unit_kind(record.unit_kind):
        raise ValueError(
            f"{record.concept!r} is unit_kind={record.unit_kind!r}; "
            "expected a monetary or per-share fact"
        )
    money = Money.from_value(record.value, record.currency)
    if money is None:
        LOGGER.warning(
            "dropping monetary fact with unusable currency: "
            "concept=%s end_date=%s currency=%r",
            record.concept,
            record.end_date,
            record.currency,
        )
        return None
    return MonetaryFact(record=record, money=money)


def to_scalar_fact(record: FactRecord) -> ScalarFact:
    """Mint a :class:`ScalarFact`.

    Raises ``ValueError`` when ``record`` is a monetary/per-share fact: a metric
    asked for a scalar on a currency-bearing concept (a programming error).
    """

    if is_monetary_unit_kind(record.unit_kind):
        raise ValueError(
            f"{record.concept!r} is unit_kind={record.unit_kind!r}; "
            "expected a non-monetary (scalar/count) fact"
        )
    return ScalarFact(record=record)


@runtime_checkable
class FactReader(Protocol):
    """The kind-tagged fact interface metrics depend on.

    Any object exposing these four accessors satisfies it -- the wrapping
    :class:`RegionFactsRepository`, its batch-cache subclass, and test fakes --
    so metrics can be typed against this protocol rather than a concrete repo.
    """

    def latest_monetary_fact(
        self, symbol: str, concept: str
    ) -> Optional[MonetaryFact]: ...

    def latest_scalar_fact(self, symbol: str, concept: str) -> Optional[ScalarFact]: ...

    def monetary_facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[MonetaryFact]: ...

    def scalar_facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[ScalarFact]: ...


class TypedFactReaderMixin:
    """Kind-tagged fact accessors implemented over a class's own raw readers.

    A class that supplies :meth:`latest_fact` and :meth:`facts_for_concept`
    gains the four typed accessors by inheriting this mixin. Both the production
    :class:`RegionFactsRepository` and the in-memory test fact sources use it, so
    the ``Money``-minting boundary lives in exactly one place; defining the
    accessors *in terms of* ``self.latest_fact`` / ``self.facts_for_concept``
    also means a subclass that overrides only the raw readers (e.g. the batch
    cache) inherits correct kind-tagged behaviour for free.

    Concrete subclasses MUST override the two raw readers; the bodies here exist
    only to give the accessors a typed surface to call and to fail loudly if a
    subclass forgets.
    """

    def latest_fact(self, symbol: str, concept: str) -> Optional[FactRecord]:
        """Return the most recent raw record for ``concept`` (subclass hook)."""

        raise NotImplementedError

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        """Return raw records for ``concept`` newest-first (subclass hook)."""

        raise NotImplementedError

    # -- kind-tagged accessors (the metric-facing read boundary) -----------

    def latest_monetary_fact(self, symbol: str, concept: str) -> Optional[MonetaryFact]:
        record = self.latest_fact(symbol, concept)
        return to_monetary_fact(record) if record is not None else None

    def latest_scalar_fact(self, symbol: str, concept: str) -> Optional[ScalarFact]:
        record = self.latest_fact(symbol, concept)
        return to_scalar_fact(record) if record is not None else None

    def monetary_facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[MonetaryFact]:
        facts: List[MonetaryFact] = []
        for record in self.facts_for_concept(
            symbol, concept, fiscal_period=fiscal_period, limit=limit
        ):
            monetary = to_monetary_fact(record)
            if monetary is not None:
                facts.append(monetary)
        return facts

    def scalar_facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[ScalarFact]:
        return [
            to_scalar_fact(record)
            for record in self.facts_for_concept(
                symbol, concept, fiscal_period=fiscal_period, limit=limit
            )
        ]


class RegionFactsRepository(TypedFactReaderMixin):
    """Wrap a raw fact source with the kind-tagged read interface metrics use.

    The legacy raw readers (``latest_fact`` / ``facts_for_concept``) delegate to
    the wrapped source; the typed accessors are inherited from
    :class:`TypedFactReaderMixin`. ``__getattr__`` proxies any other attribute
    (e.g. ``ticker_currency``) to the wrapped source.
    """

    def __init__(self, repo: RawFactSource) -> None:
        self._repo = repo

    def latest_fact(
        self,
        symbol: str,
        concept: str,
    ) -> Optional[FactRecord]:
        return self._repo.latest_fact(symbol, concept)

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        return self._repo.facts_for_concept(
            symbol,
            concept,
            fiscal_period=fiscal_period,
            limit=limit,
        )

    def __getattr__(self, name: str) -> Any:
        # Transparent proxy: forward any attribute not defined on this view to
        # the wrapped repo. ``Any`` is the correct type for a dynamic forwarder
        # (the attribute could be anything the repo exposes), not a way to
        # silence the checker.
        return getattr(self._repo, name)


__all__ = [
    "FactReader",
    "FactView",
    "MonetaryFact",
    "RawFactSource",
    "RegionFactsRepository",
    "ScalarFact",
    "TypedFactReaderMixin",
    "to_monetary_fact",
    "to_scalar_fact",
]
