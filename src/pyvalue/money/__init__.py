"""Monetary domain: the Money value type, currency-aware conversion, and FX rates.

This package groups the currency-carrying :class:`Money` type and its
conversion helpers (``conversion``) together with the FX rate lookup/refresh
service (``fx``). The conversion API is re-exported here so call sites keep
using ``from pyvalue.money import Money``; the FX service is reached at its
sub-module, ``from pyvalue.money.fx import FXService``.

``currency`` deliberately stays at the package root (``pyvalue.currency``): it
is foundational vocabulary the persistence layer depends on, so folding it in
here would create a ``persistence -> money -> persistence`` import cycle.

Author: Emre Tezel
"""

from .conversion import (
    CurrencyMismatchError,
    Money,
    align_money_values,
    choose_target_currency,
    convert_money_value,
    currencies_match,
    ephemeral_fx_database_path,
    fx_converter_for_context,
    fx_service_for_context,
    normalize_fact_value,
    normalize_money_value,
)

__all__ = [
    "CurrencyMismatchError",
    "Money",
    "align_money_values",
    "choose_target_currency",
    "convert_money_value",
    "currencies_match",
    "ephemeral_fx_database_path",
    "fx_converter_for_context",
    "fx_service_for_context",
    "normalize_fact_value",
    "normalize_money_value",
]
