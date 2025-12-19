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

    records = normalizer.normalize(payload, symbol="AAPL.US", cik="CIK0000320193")

    concepts = {rec.concept for rec in records}
    assert concepts == {"NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic"}
    net_income = next(rec for rec in records if rec.concept == "NetIncomeLoss")
    derived = next(rec for rec in records if rec.concept == "NetIncomeLossAvailableToCommonStockholdersBasic")
    assert net_income.value == 123.45
    assert net_income.end_date == "2023-09-30"
    assert derived.value == 123.45
    assert derived.end_date == "2023-09-30"


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
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebtNoncurrent": {
                    "units": {
                        "USD": [
                            {
                                "val": 100,
                                "fp": "Q1",
                                "end": "2024-03-31",
                                "form": "10-Q",
                                "filed": "2024-05-01",
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
                                "end": "2024-03-31",
                                "form": "10-Q",
                                "filed": "2024-05-01",
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
        if rec.concept == "LongTermDebt" and rec.end_date == "2024-03-31" and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 120


def test_normalizer_derives_earnings_per_share_from_diluted():
    payload = {
        "facts": {
            "us-gaap": {
                "DilutedEPS": {
                    "units": {
                        "USD": [
                            {
                                "val": 2.0,
                                "fp": "Q1",
                                "end": "2024-03-31",
                                "form": "10-Q",
                                "filed": "2024-05-01",
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
                                "end": "2024-03-31",
                                "form": "10-Q",
                                "filed": "2024-05-01",
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
        if rec.concept == "EarningsPerShare" and rec.end_date == "2024-03-31" and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 2.0


def test_normalizer_derives_intangibles_excluding_goodwill_from_net():
    payload = {
        "facts": {
            "us-gaap": {
                "IntangibleAssetsNet": {
                    "units": {
                        "USD": [
                            {
                                "val": 45.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 45.0


def test_normalizer_derives_stockholders_equity_from_equity_including_noncontrolling():
    payload = {
        "facts": {
            "us-gaap": {
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": {
                    "units": {
                        "USD": [
                            {
                                "val": 500.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 500.0


def test_normalizer_derives_common_shares_from_entity_shares():
    payload = {
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "val": 1200,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 1200.0


def test_normalizer_derives_operating_cash_flow_from_continuing_operations():
    payload = {
        "facts": {
            "us-gaap": {
                "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": {
                    "units": {
                        "USD": [
                            {
                                "val": 250.0,
                                "fp": "Q1",
                                "end": "2024-03-31",
                                "form": "10-Q",
                                "filed": "2024-05-01",
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
        and rec.end_date == "2024-03-31"
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == 250.0


def test_normalizer_derives_capex_from_payments_to_acquire_ppe():
    payload = {
        "facts": {
            "us-gaap": {
                "PaymentsToAcquirePropertyPlantAndEquipment": {
                    "units": {
                        "USD": [
                            {
                                "val": -60.0,
                                "fp": "Q1",
                                "end": "2024-03-31",
                                "form": "10-Q",
                                "filed": "2024-05-01",
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
        and rec.end_date == "2024-03-31"
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == -60.0


def test_normalizer_derives_operating_income_from_income_from_operations():
    payload = {
        "facts": {
            "us-gaap": {
                "IncomeFromOperations": {
                    "units": {
                        "USD": [
                            {
                                "val": 180.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
                            }
                        ]
                    }
                }
            }
        }
    }
    normalizer = SECFactsNormalizer(concepts=["IncomeFromOperations"])

    records = normalizer.normalize(payload, symbol="TEST", cik="CIK0000")

    derived = [
        rec
        for rec in records
        if rec.concept == "OperatingIncomeLoss"
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 180.0


def test_normalizer_derives_ppe_from_net_property_plant_and_equipment():
    payload = {
        "facts": {
            "us-gaap": {
                "NetPropertyPlantAndEquipment": {
                    "units": {
                        "USD": [
                            {
                                "val": 750.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 750.0


def test_normalizer_derives_net_income_available_to_common_from_net_income():
    payload = {
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "val": 300.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 300.0


def test_normalizer_derives_common_stockholders_equity_from_stockholders_equity():
    payload = {
        "facts": {
            "us-gaap": {
                "StockholdersEquity": {
                    "units": {
                        "USD": [
                            {
                                "val": 900.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 900.0


def test_normalizer_derives_intangibles_excluding_goodwill_from_components():
    payload = {
        "facts": {
            "us-gaap": {
                "FiniteLivedIntangibleAssetsNet": {
                    "units": {
                        "USD": [
                            {
                                "val": 30.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 45.0


def test_normalizer_derives_net_income_available_to_common_from_diluted():
    payload = {
        "facts": {
            "us-gaap": {
                "NetIncomeLossAvailableToCommonStockholdersDiluted": {
                    "units": {
                        "USD": [
                            {
                                "val": 120.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 120.0


def test_normalizer_derives_capex_from_low_priority_tag():
    payload = {
        "facts": {
            "us-gaap": {
                "PaymentsToAcquireOtherProductiveAssets": {
                    "units": {
                        "USD": [
                            {
                                "val": -12.0,
                                "fp": "Q1",
                                "end": "2024-03-31",
                                "form": "10-Q",
                                "filed": "2024-05-01",
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
        and rec.end_date == "2024-03-31"
        and rec.fiscal_period == "Q1"
    ]
    assert len(derived) == 1
    assert derived[0].value == -12.0


def test_normalizer_derives_ppe_from_lease_and_ppe_rollup():
    payload = {
        "facts": {
            "us-gaap": {
                "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization": {
                    "units": {
                        "USD": [
                            {
                                "val": 520.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 520.0


def test_normalizer_derives_common_shares_from_shares_outstanding():
    payload = {
        "facts": {
            "dei": {
                "SharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "val": 750,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 750.0


def test_normalizer_derives_stockholders_equity_from_common_equity():
    payload = {
        "facts": {
            "us-gaap": {
                "CommonStockholdersEquity": {
                    "units": {
                        "USD": [
                            {
                                "val": 410.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 410.0


def test_normalizer_derives_long_term_debt_from_other_long_term_debt():
    payload = {
        "facts": {
            "us-gaap": {
                "OtherLongTermDebt": {
                    "units": {
                        "USD": [
                            {
                                "val": 300.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 340.0


def test_normalizer_derives_long_term_debt_from_lease_including_current():
    payload = {
        "facts": {
            "us-gaap": {
                "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities": {
                    "units": {
                        "USD": [
                            {
                                "val": 510.0,
                                "fp": "FY",
                                "end": "2023-12-31",
                                "form": "10-K",
                                "filed": "2024-02-01",
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
        and rec.end_date == "2023-12-31"
        and rec.fiscal_period == "FY"
    ]
    assert len(derived) == 1
    assert derived[0].value == 510.0
