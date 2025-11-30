"""CLI path resolution helpers.

Author: Emre Tezel
"""

from pathlib import Path

from pyvalue.cli import _resolve_database_path


def test_resolve_database_path_falls_back_to_repo_data(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    resolved = _resolve_database_path("data/pyvalue.db")

    assert resolved.name == "pyvalue.db"
    assert resolved.exists()
    assert Path("data/pyvalue.db").resolve() != resolved
