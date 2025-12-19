"""Tests for purge-us-nonfilers CLI."""

import json

from pyvalue.cli import cmd_purge_us_nonfilers
from pyvalue.storage import FundamentalsRepository, UniverseRepository
from pyvalue.universe import Listing


def _seed_universe(db_path):
    universe = UniverseRepository(db_path)
    universe.initialize_schema()
    universe.replace_universe(
        [
            Listing(symbol="FILER.US", security_name="Filer", exchange="NASDAQ"),
            Listing(symbol="NONFILER.US", security_name="NonFiler", exchange="NYSE"),
        ],
        region="US",
    )


def _seed_company_facts(db_path):
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    payload = {
        "facts": {
            "us-gaap": {
                "Assets": {
                    "units": {
                        "USD": [
                            {"end": "2024-12-31", "val": 1, "accn": "000", "fy": 2024, "fp": "FY", "form": "10-K", "filed": "2025-01-01"}
                        ]
                    }
                }
            }
        }
    }
    repo.upsert("SEC", "FILER.US", payload, region="US")


def test_purge_us_nonfilers_dry_run(tmp_path, capsys):
    db_path = tmp_path / "purge.db"
    _seed_universe(db_path)
    _seed_company_facts(db_path)

    exit_code = cmd_purge_us_nonfilers(database=str(db_path), apply=False)

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "NONFILER.US" in output
    # Ensure listings intact
    universe = UniverseRepository(db_path)
    with universe._connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM listings WHERE region='US'").fetchone()[0]
    assert count == 2


def test_purge_us_nonfilers_apply(tmp_path):
    db_path = tmp_path / "purge_apply.db"
    _seed_universe(db_path)
    _seed_company_facts(db_path)

    exit_code = cmd_purge_us_nonfilers(database=str(db_path), apply=True)

    assert exit_code == 0
    universe = UniverseRepository(db_path)
    with universe._connect() as conn:
        symbols = [row[0] for row in conn.execute("SELECT symbol FROM listings WHERE region='US'")]
    assert symbols == ["FILER.US"]
