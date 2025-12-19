"""AssetsCurrent derivation via SEC normalization."""

from pyvalue.normalization import SECFactsNormalizer


def _quarter_entry(value: float) -> dict:
    return {
        "val": value,
        "fp": "Q1",
        "end": "2024-03-31",
        "form": "10-Q",
        "filed": "2024-05-01",
    }


def test_normalizer_derives_assets_current_from_components():
    payload = {
        "facts": {
            "us-gaap": {
                "CashAndCashEquivalentsAtCarryingValue": {
                    "units": {"USD": [_quarter_entry(50.0)]}
                },
                "InventoryNet": {"units": {"USD": [_quarter_entry(25.0)]}},
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "AssetsCurrent" and rec.end_date == "2024-03-31" and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 75.0


def test_normalizer_skips_assets_current_derivation_when_total_present():
    payload = {
        "facts": {
            "us-gaap": {
                "AssetsCurrent": {"units": {"USD": [_quarter_entry(100.0)]}},
                "CashAndCashEquivalentsAtCarryingValue": {
                    "units": {"USD": [_quarter_entry(50.0)]}
                },
                "InventoryNet": {"units": {"USD": [_quarter_entry(25.0)]}},
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    assets = [
        rec
        for rec in records
        if rec.concept == "AssetsCurrent" and rec.end_date == "2024-03-31" and rec.fiscal_period == "Q1"
    ]
    assert len(assets) == 1
    assert assets[0].value == 100.0
