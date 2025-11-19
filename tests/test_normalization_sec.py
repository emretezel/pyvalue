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
                                    "form": "10-K",
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


def test_normalizer_handles_quarters_that_cross_calendar_years():
    payload = {
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "val": 100,
                                "fy": 2023,
                                "fp": "Q1",
                                "start": "2022-10-01",
                                "end": "2022-12-31",
                                "filed": "2023-02-01",
                                "form": "10-Q",
                            },
                            {
                                "val": 180,
                                "fy": 2023,
                                "fp": "Q2",
                                "start": "2023-01-01",
                                "end": "2023-03-31",
                                "filed": "2023-05-01",
                                "form": "10-Q",
                            },
                            {
                                "val": 250,
                                "fy": 2023,
                                "fp": "Q3",
                                "start": "2023-04-01",
                                "end": "2023-06-30",
                                "filed": "2023-08-01",
                                "form": "10-Q",
                            },
                            {
                                "val": 400,
                                "fy": 2023,
                                "fp": "FY",
                                "start": "2022-10-01",
                                "end": "2023-09-30",
                                "filed": "2023-10-31",
                                "form": "10-K",
                            },
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["NetIncomeLoss"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    values = {rec.fiscal_period: rec.value for rec in records}
    assert values["FY"] == 400
    assert values["Q1"] == 100
    assert values["Q2"] == 80
    assert values["Q3"] == 70
    assert values["Q4"] == 150


def test_normalizer_drops_beginning_balance_duplicates_for_instant_facts():
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 100,
                                "fp": "Q1",
                                "end": "2023-09-30",
                                "form": "10-Q",
                                "filed": "2024-01-30",
                                "accn": "accn-q1",
                            },
                            {
                                "val": 150,
                                "fp": "Q1",
                                "end": "2023-12-30",
                                "form": "10-Q",
                                "filed": "2024-01-30",
                                "accn": "accn-q1",
                            },
                            {
                                "val": 200,
                                "fp": "Q2",
                                "end": "2023-09-30",
                                "form": "10-Q",
                                "filed": "2024-04-30",
                                "accn": "accn-q2",
                            },
                            {
                                "val": 220,
                                "fp": "Q2",
                                "end": "2024-03-30",
                                "form": "10-Q",
                                "filed": "2024-04-30",
                                "accn": "accn-q2",
                            },
                            {
                                "val": 250,
                                "fp": "Q3",
                                "end": "2023-09-30",
                                "form": "10-Q",
                                "filed": "2024-07-30",
                                "accn": "accn-q3",
                            },
                            {
                                "val": 260,
                                "fp": "Q3",
                                "end": "2024-06-29",
                                "form": "10-Q",
                                "filed": "2024-07-30",
                                "accn": "accn-q3",
                            },
                            {
                                "val": 300,
                                "fp": "FY",
                                "end": "2024-09-28",
                                "form": "10-K",
                                "filed": "2024-10-30",
                            },
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["LongTermDebtCurrent"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    by_period = {(rec.fiscal_period, rec.end_date): rec.value for rec in records}
    assert by_period["FY", "2024-09-28"] == 300
    assert by_period["Q1", "2023-12-30"] == 150
    assert by_period["Q2", "2024-03-30"] == 220
    assert by_period["Q3", "2024-06-29"] == 260
    assert by_period["Q4", "2024-09-28"] == 300


def test_normalizer_includes_entity_common_shares():
    payload = {
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "val": "1000",
                                "fy": 2023,
                                "fp": "FY",
                                "end": "2023-09-30",
                                "filed": "2023-10-30",
                                "frame": "CY2023",
                                "form": "10-K",
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer()

    records = normalizer.normalize(payload, symbol="AAA", cik="CIK0000")
    concepts = {rec.concept for rec in records}
    assert "EntityCommonStockSharesOutstanding" in concepts
