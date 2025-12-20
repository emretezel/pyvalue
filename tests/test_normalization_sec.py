"""Tests for SEC facts normalization helpers.

Author: Emre Tezel
"""
from datetime import date, timedelta

from pyvalue.normalization import SECFactsNormalizer


def _recent_date(days: int = 10) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def test_normalizer_emits_records_for_target_concepts():
    recent = _recent_date()
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
                                    "end": recent,
                                    "accn": "000",
                                    "filed": recent,
                                    "frame": f"CY{recent[:4]}",
                                    "form": "10-K",
                                }
                            ]
                        }
                    },
                "Unused": {
                    "units": {"USD": [{"val": 1, "end": recent}]}
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["NetIncomeLoss"])

    records = normalizer.normalize(payload, symbol="AAPL.US", cik="CIK0000320193")

    concepts = {rec.concept for rec in records}
    assert concepts == {"NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic"}
    net_income = next(rec for rec in records if rec.concept == "NetIncomeLoss")
    derived = next(rec for rec in records if rec.concept == "NetIncomeLossAvailableToCommonStockholdersBasic")
    assert net_income.value == 123.45
    assert net_income.end_date == recent
    assert derived.value == 123.45
    assert derived.end_date == recent


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


def test_normalizer_derives_long_term_debt_from_noncurrent_and_current():
    recent = (date.today() - timedelta(days=10)).isoformat()
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebtNoncurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 100,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 20,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["LongTermDebtNoncurrent", "LongTermDebtCurrent"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LongTermDebt" and rec.end_date == recent and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 120


def test_normalizer_prefers_long_term_debt_tag_over_fallbacks():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebt": {
                    "units": {
                        "USD": [
                            {
                                "val": 200,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtNoncurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 180,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 30,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=["LongTermDebt", "LongTermDebtNoncurrent", "LongTermDebtCurrent"]
    )

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LongTermDebt" and rec.end_date == recent and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 200


def test_normalizer_derives_earnings_per_share_from_diluted():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "DilutedEPS": {
                    "units": {
                        "USD": [
                            {
                                "val": 2.0,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "EarningsPerShareBasic": {
                    "units": {
                        "USD": [
                            {
                                "val": 1.5,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["DilutedEPS", "EarningsPerShareBasic"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "EarningsPerShare" and rec.end_date == recent and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 2.0


def test_normalizer_derives_earnings_per_share_from_stale_fallback():
    stale = (date.today() - timedelta(days=400)).isoformat()
    payload = {
        "facts": {
            "us-gaap": {
                "EarningsPerShareDiluted": {
                    "units": {
                        "USD": [
                            {
                                "val": 3.2,
                                "fp": "FY",
                                "end": stale,
                                "form": "10-K",
                                "filed": stale,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["EarningsPerShareDiluted"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "EarningsPerShare" and rec.end_date == stale and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 3.2


def test_normalizer_derives_intangibles_excluding_goodwill_from_net():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "IntangibleAssetsNet": {
                    "units": {
                        "USD": [
                            {
                                "val": 45.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["IntangibleAssetsNet"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "IntangibleAssetsNetExcludingGoodwill"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 45.0


def test_normalizer_derives_stockholders_equity_from_equity_including_noncontrolling():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": {
                    "units": {
                        "USD": [
                            {
                                "val": 500.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "StockholdersEquity"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 500.0


def test_normalizer_derives_common_shares_from_entity_shares():
    recent = _recent_date()
    payload = {
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "val": 1200,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["EntityCommonStockSharesOutstanding"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "CommonStockSharesOutstanding"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 1200.0


def test_normalizer_derives_operating_cash_flow_from_continuing_operations():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": {
                    "units": {
                        "USD": [
                            {
                                "val": 250.0,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "NetCashProvidedByUsedInOperatingActivities"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 250.0


def test_normalizer_derives_capex_from_payments_to_acquire_ppe():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "PaymentsToAcquirePropertyPlantAndEquipment": {
                    "units": {
                        "USD": [
                            {
                                "val": -60.0,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["PaymentsToAcquirePropertyPlantAndEquipment"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "CapitalExpenditures"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == -60.0


def test_normalizer_derives_operating_income_from_income_before_taxes():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": {
                    "units": {
                        "USD": [
                            {
                                "val": 180.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"]
    )

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "OperatingIncomeLoss"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 180.0


def test_normalizer_derives_ppe_from_net_property_plant_and_equipment():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "NetPropertyPlantAndEquipment": {
                    "units": {
                        "USD": [
                            {
                                "val": 750.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["NetPropertyPlantAndEquipment"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "PropertyPlantAndEquipmentNet"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 750.0


def test_normalizer_derives_net_income_available_to_common_from_net_income():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "val": 300.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["NetIncomeLoss"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "NetIncomeLossAvailableToCommonStockholdersBasic"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 300.0


def test_normalizer_derives_common_stockholders_equity_from_stockholders_equity():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "StockholdersEquity": {
                    "units": {
                        "USD": [
                            {
                                "val": 900.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["StockholdersEquity"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "CommonStockholdersEquity"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 900.0


def test_normalizer_derives_intangibles_excluding_goodwill_from_components():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "FiniteLivedIntangibleAssetsNet": {
                    "units": {
                        "USD": [
                            {
                                "val": 30.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "IndefiniteLivedIntangibleAssetsExcludingGoodwill": {
                    "units": {
                        "USD": [
                            {
                                "val": 15.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=[
            "FiniteLivedIntangibleAssetsNet",
            "IndefiniteLivedIntangibleAssetsExcludingGoodwill",
        ]
    )

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "IntangibleAssetsNetExcludingGoodwill"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 45.0


def test_normalizer_derives_net_income_available_to_common_from_diluted():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "NetIncomeLossAvailableToCommonStockholdersDiluted": {
                    "units": {
                        "USD": [
                            {
                                "val": 120.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "PreferredStockDividendsIncomeStatementImpact": {
                    "units": {
                        "USD": [
                            {
                                "val": 15.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=[
            "NetIncomeLossAvailableToCommonStockholdersDiluted",
            "PreferredStockDividendsIncomeStatementImpact",
        ]
    )

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "NetIncomeLossAvailableToCommonStockholdersBasic"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 120.0


def test_normalizer_derives_capex_from_low_priority_tag():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "PaymentsToAcquireOtherProductiveAssets": {
                    "units": {
                        "USD": [
                            {
                                "val": -12.0,
                                "fp": "Q1",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["PaymentsToAcquireOtherProductiveAssets"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "CapitalExpenditures"
        and rec.end_date == recent
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == -12.0


def test_normalizer_derives_ppe_from_lease_and_ppe_rollup():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization": {
                    "units": {
                        "USD": [
                            {
                                "val": 520.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=[
            "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization"
        ]
    )

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "PropertyPlantAndEquipmentNet"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 520.0


def test_normalizer_derives_common_shares_from_shares_outstanding():
    recent = _recent_date()
    payload = {
        "facts": {
            "dei": {
                "SharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "val": 750,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["SharesOutstanding"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "CommonStockSharesOutstanding"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 750.0


def test_normalizer_derives_stockholders_equity_from_common_equity():
    recent = _recent_date()
    payload = {
        "facts": {
            "us-gaap": {
                "CommonStockholdersEquity": {
                    "units": {
                        "USD": [
                            {
                                "val": 410.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["CommonStockholdersEquity"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "StockholdersEquity"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 410.0


def test_normalizer_derives_long_term_debt_from_other_long_term_debt():
    recent = (date.today() - timedelta(days=10)).isoformat()
    payload = {
        "facts": {
            "us-gaap": {
                "OtherLongTermDebt": {
                    "units": {
                        "USD": [
                            {
                                "val": 300.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 40.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["OtherLongTermDebt", "LongTermDebtCurrent"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LongTermDebt"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 340.0


def test_normalizer_derives_long_term_debt_from_lease_including_current():
    recent = (date.today() - timedelta(days=10)).isoformat()
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities": {
                    "units": {
                        "USD": [
                            {
                                "val": 510.0,
                                "fp": "FY",
                                "end": recent,
                                "form": "10-K",
                                "filed": recent,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=["LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities"]
    )

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LongTermDebt"
        and rec.end_date == recent
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 510.0


def test_normalizer_derives_long_term_debt_from_other_liabilities_noncurrent():
    recent = (date.today() - timedelta(days=10)).isoformat()
    payload = {
        "facts": {
            "us-gaap": {
                "OtherLiabilitiesNoncurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 75.0,
                                "fp": "Q3",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 5.0,
                                "fp": "Q3",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["OtherLiabilitiesNoncurrent", "LongTermDebtCurrent"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LongTermDebt" and rec.end_date == recent and rec.fiscal_period == "Q3"
    ]
    assert len(derived) == 1
    assert derived[0].value == 80.0


def test_normalizer_derives_long_term_debt_from_operating_lease_liability_noncurrent():
    recent = (date.today() - timedelta(days=10)).isoformat()
    payload = {
        "facts": {
            "us-gaap": {
                "OperatingLeaseLiabilityNoncurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 60.0,
                                "fp": "Q2",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
                "LongTermDebtCurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 4.0,
                                "fp": "Q2",
                                "end": recent,
                                "form": "10-Q",
                                "filed": recent,
                            }
                        ]
                    }
                },
            }
        }
    }
    normalizer = SECFactsNormalizer(
        concepts=["OperatingLeaseLiabilityNoncurrent", "LongTermDebtCurrent"]
    )

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "LongTermDebt" and rec.end_date == recent and rec.fiscal_period == "Q2"
    ]
    assert len(derived) == 1
    assert derived[0].value == 64.0


def test_normalizer_skips_stale_long_term_debt_records():
    stale = (date.today() - timedelta(days=400)).isoformat()
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebtNoncurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 100.0,
                                "fp": "FY",
                                "end": stale,
                                "form": "10-K",
                                "filed": stale,
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["LongTermDebtNoncurrent"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [rec for rec in records if rec.concept == "LongTermDebt" and rec.end_date == stale]
    assert not derived
