"""LiabilitiesCurrent derivation via SEC normalization."""

from pyvalue.normalization import SECFactsNormalizer


def _quarter_entry(value: float) -> dict:
    return {
        "val": value,
        "fp": "Q1",
        "end": "2024-03-31",
        "form": "10-Q",
        "filed": "2024-05-01",
    }


def test_normalizer_derives_liabilities_current_from_components():
    payload = {
        "facts": {
            "us-gaap": {
                "AccountsPayableCurrent": {"units": {"USD": [_quarter_entry(15.0)]}},
                "AccruedLiabilitiesCurrent": {"units": {"USD": [_quarter_entry(5.0)]}},
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent" and rec.end_date == "2024-03-31" and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 20.0


def test_normalizer_skips_liabilities_current_derivation_when_total_present():
    payload = {
        "facts": {
            "us-gaap": {
                "LiabilitiesCurrent": {"units": {"USD": [_quarter_entry(40.0)]}},
                "AccountsPayableCurrent": {"units": {"USD": [_quarter_entry(15.0)]}},
                "AccruedLiabilitiesCurrent": {"units": {"USD": [_quarter_entry(5.0)]}},
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="TEST.US", cik="CIK0000")

    liabilities = [
        rec
        for rec in records
        if rec.concept == "LiabilitiesCurrent" and rec.end_date == "2024-03-31" and rec.fiscal_period == "Q1"
    ]
    assert len(liabilities) == 1
    assert liabilities[0].value == 40.0
