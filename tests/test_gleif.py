"""Tests for GLEIF ingestion helpers."""

from pyvalue.ingestion import GLEIFClient


def test_isin_to_company_number_parses_rows():
    golden = """LEI,RegistrationAuthorityID,RegistrationAuthorityEntityID
LEI1,RA000407,00000001
LEI2,RA000585,00000002
LEI3,OTHER,123
"""
    isin_csv = """ISIN,LEI
GB0001,LEI1
GB0002,LEI2
GB0003,LEI3
"""
    client = GLEIFClient(golden_fetcher=lambda: golden, isin_fetcher=lambda: isin_csv)
    mapping = client.isin_to_company_number(golden, isin_csv)

    assert mapping["GB0001"]["company_number"] == "00000001"
    assert mapping["GB0002"]["company_number"] == "00000002"
    assert "GB0003" not in mapping
