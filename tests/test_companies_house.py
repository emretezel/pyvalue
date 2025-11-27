"""Tests for Companies House ingestion helpers.

Author: Emre Tezel
"""

import json

import pytest

from pyvalue.ingestion import CompaniesHouseClient


def test_fetch_company_profile_uses_fetcher(monkeypatch):
    payload = {"company_name": "Example Ltd", "company_number": "00000000"}
    client = CompaniesHouseClient(api_key="key", fetcher=lambda number: json.dumps(payload))

    data = client.fetch_company_profile("00000000")

    assert data["company_name"] == "Example Ltd"


def test_client_requires_api_key():
    with pytest.raises(ValueError):
        CompaniesHouseClient(api_key="")
