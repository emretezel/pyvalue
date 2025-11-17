"""Tests for SEC facts normalization helpers.

Author: Emre Tezel
"""
from pyvalue.normalization import SECFactsNormalizer


def test_normalizer_emits_records_for_target_concepts():
    payload = {
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "val": "123.45",
                                "fy": 2023,
                                "fp": "FY",
                                "end": "2023-09-30",
                                "accn": "000",
                                "filed": "2023-10-30",
                                "frame": "CY2023",
                            }
                        ]
                    }
                },
                "Unused": {
                    "units": {"USD": [{"val": 1, "end": "2023-09-30"}]}
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["NetIncomeLoss"])

    records = normalizer.normalize(payload, symbol="AAPL", cik="CIK0000320193")

    assert len(records) == 1
    rec = records[0]
    assert rec.concept == "NetIncomeLoss"
    assert rec.value == 123.45
    assert rec.end_date == "2023-09-30"
