"""AssetsCurrent derivation via SEC normalization."""

from datetime import date, timedelta

from pyvalue.normalization import SECFactsNormalizer


def _recent_date(days: int = 10) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _quarter_entry(value: float, end_date: str) -> dict:
    return {
        "val": value,
        "fp": "Q1",
        "end": end_date,
        "form": "10-Q",
        "filed": end_date,
    }


def test_normalizer_derives_assets_current_from_components():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "CashAndCashEquivalentsAtCarryingValue": {
                    "units": {"USD": [_quarter_entry(50.0, recent)]}
                },
                "InventoryNet": {"units": {"USD": [_quarter_entry(25.0, recent)]}},
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "AssetsCurrent" and rec.end_date == recent and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 75.0


def test_normalizer_skips_assets_current_derivation_when_total_present():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "AssetsCurrent": {"units": {"USD": [_quarter_entry(100.0, recent)]}},
                "CashAndCashEquivalentsAtCarryingValue": {
                    "units": {"USD": [_quarter_entry(50.0, recent)]}
                },
                "InventoryNet": {"units": {"USD": [_quarter_entry(25.0, recent)]}},
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    assets = [
        rec
        for rec in records
        if rec.concept == "AssetsCurrent" and rec.end_date == recent and rec.fiscal_period == "Q1"
    ]
    assert len(assets) == 1
    assert assets[0].value == 100.0
