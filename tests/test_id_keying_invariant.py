"""Regression guards for the id-keying + storage-boundary invariants.

These tests make two architectural rules mechanically enforceable so they cannot
silently rot (see the ``downstream-must-be-id-keyed`` project memory):

1. **All DB access lives in ``persistence/storage/``.** No module outside that
   package may import ``sqlite3`` or touch a connection.
2. **Symbol/exchange-code -> id lookups stay on a small allowlist.** A new
   ``WHERE provider_symbol = ?`` (etc.) anywhere else fails this test until the
   method is justified and added to the allowlist -- keeping the surface auditable.

Author: Emre Tezel
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src" / "pyvalue"
_STORAGE = _SRC / "persistence" / "storage"

# A module touching a sqlite connection / running SQL betrays the boundary.
_DB_ACCESS = re.compile(
    r"(?:import sqlite3|sqlite3\.|_connect\(|\.execute\(|\.executemany\("
    r"|\.executescript\(|\.commit\(|\.rollback\(|\.cursor\(|open_persistent_connection)"
)

# The symbol/exchange-code -> id resolution predicate.
_SYMBOL_PREDICATE = re.compile(
    r"(?:l\.symbol|provider_symbol|exchange_code|provider_exchange_code)"
    r"\s*(?:=\s*\?|IN\s*\()"
)

# The ONLY storage methods allowed to resolve a symbol/exchange-code to an id:
# the three legitimate boundaries (catalog creation in refresh-supported-*,
# CLI scope resolution, and the ingest/market-data eligibility selection), plus
# a handful of dead-in-production symbol accessors slated for deletion in the
# Phase-3 cleanup. Anything NOT here is a violation. (Shrinks as Phase 3/4 land.)
_ALLOWED_SYMBOL_METHODS = frozenset(
    {
        # catalog creation -- refresh-supported-{exchanges,tickers}
        "_resolve_provider_exchange",
        "_ensure_provider_listing",
        "_ensure",
        "_load_by_exchange_and_symbol",
        # CLI scope resolution + ingest/market-data eligibility selection
        "_listing_pair_filter",
        "list_supported_listings",
        "list_for_provider",
        "count_for_provider",
        "list_eligible_for_market_data",
        "_apply_scope_filters",
        # reconcile (listing-status) + reporting grouped by exchange
        "_select_rows",
        "progress_by_exchange",
        "recent_failures",
        "market_data_progress_by_exchange",
        "recent_market_data_failures",
        # dead-in-production symbol accessors (deleted in Phase 3; allowlisted
        # until then so this guard can land first).
        "fetch",
        "fetch_by_symbol",
        "delete_symbols",  # FundamentalsNormalizationStateRepository (P3-3)
    }
)

# Schema-engine + schema-doc tooling legitimately run arbitrary DDL/SQL; they live
# in storage/ but are exempt from the symbol-predicate allowlist (they are not the
# read/write repository layer).
_PREDICATE_EXEMPT_FILES = {"migrations.py", "database_review_docs.py"}


def _enclosing_function(tree: ast.Module, lineno: int) -> str:
    enclosing = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.lineno <= lineno <= (node.end_lineno or node.lineno)
    ]
    return enclosing[-1] if enclosing else "<module>"


def test_db_access_confined_to_storage_package() -> None:
    """No module outside ``persistence/storage/`` imports sqlite3 or runs SQL."""
    offenders: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        if _STORAGE in path.parents:
            continue
        for i, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if _DB_ACCESS.search(line):
                offenders.append(f"{path.relative_to(_SRC)}:{i}: {stripped}")
    assert not offenders, (
        "DB access (sqlite3 / connection) found outside persistence/storage/:\n"
        + "\n".join(offenders)
    )


def test_symbol_lookups_confined_to_allowlist() -> None:
    """Every symbol/exchange-code predicate in the repo layer is on the allowlist."""
    offenders: list[str] = []
    for path in sorted(_STORAGE.glob("*.py")):
        if path.name in _PREDICATE_EXEMPT_FILES:
            continue
        source = path.read_text()
        tree = ast.parse(source)
        for i, line in enumerate(source.splitlines(), 1):
            if line.strip().startswith("#"):
                continue
            if _SYMBOL_PREDICATE.search(line):
                method = _enclosing_function(tree, i)
                if method not in _ALLOWED_SYMBOL_METHODS:
                    offenders.append(f"{path.name}:{i} in {method}(): {line.strip()}")
    assert not offenders, (
        "Symbol/exchange-code lookup outside the allowlist (resolve once at a "
        "boundary, then key by id):\n" + "\n".join(offenders)
    )
