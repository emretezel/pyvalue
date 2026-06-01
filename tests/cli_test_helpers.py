"""Helpers for faking ``pyvalue.cli`` internals in tests.

Author: Emre Tezel
"""

from __future__ import annotations

import sys
from typing import Any

import pytest


def patch_cli(monkeypatch: pytest.MonkeyPatch, name: str, value: Any) -> None:
    """Replace ``name`` everywhere the ``pyvalue.cli`` package binds it.

    ``pyvalue.cli`` is a package split from a former single module, so a symbol
    such as ``EODHDFundamentalsClient`` is imported independently by several
    command sub-modules (e.g. ``cli.ingest`` and ``cli.market_data``), each
    holding its own binding. A handler reads the binding in *its* module, so a
    test that fakes such a symbol must patch every binding the code under test
    might read. This sets ``name`` on the package facade and on every loaded
    ``pyvalue.cli.*`` sub-module that defines it; ``monkeypatch`` restores each
    binding at teardown.

    Use this instead of ``monkeypatch.setattr(cli, name, value)``: the facade is
    a plain re-export, so patching it alone would not reach the handlers' own
    module globals.
    """
    import pyvalue.cli  # noqa: F401  (ensure the package + submodules are loaded)

    patched_any = False
    candidates = [sys.modules["pyvalue.cli"]]
    candidates += [
        module
        for module_name, module in list(sys.modules.items())
        if module is not None and module_name.startswith("pyvalue.cli.")
    ]
    for module in candidates:
        if name in vars(module):
            monkeypatch.setattr(module, name, value)
            patched_any = True
    if not patched_any:
        raise AttributeError(f"no pyvalue.cli module defines {name!r} to patch")
