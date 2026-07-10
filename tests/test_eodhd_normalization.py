from pathlib import Path

from pyvalue.money.fx import FXService
from pyvalue.normalization.eodhd import EODHDFactsNormalizer
from pyvalue.persistence.storage import FXRateRecord, FXRatesRepository


def test_eodhd_normalizes_ppe_net() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "propertyPlantAndEquipmentNet": 100.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    ppe_records = [r for r in records if r.concept == "PropertyPlantAndEquipmentNet"]
    assert ppe_records, "PPE net should be normalized from propertyPlantAndEquipmentNet"
    rec = ppe_records[0]
    assert rec.value == 100.0
    assert rec.end_date == "2024-12-31"
    assert rec.fiscal_period == "FY"


def test_eodhd_normalizes_assets_from_total_assets() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 250.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    assets_records = [r for r in records if r.concept == "Assets"]
    assert assets_records, "Assets should be normalized from totalAssets"
    assert assets_records[0].value == 250.0


def test_eodhd_derives_intangibles_excluding_goodwill_from_net() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "intangibleAssets": 80.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [
        r for r in records if r.concept == "IntangibleAssetsNetExcludingGoodwill"
    ]
    assert derived, (
        "IntangibleAssetsNetExcludingGoodwill should be derived from IntangibleAssetsNet"
    )
    assert derived[0].value == 80.0


def test_eodhd_derives_common_shares_from_entity_shares() -> None:
    normalizer = EODHDFactsNormalizer(concepts=["EntityCommonStockSharesOutstanding"])
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "shareIssued": 1500,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "CommonStockSharesOutstanding"]
    assert derived, (
        "CommonStockSharesOutstanding should be derived from EntityCommonStockSharesOutstanding"
    )
    assert derived[0].value == 1500.0


def test_eodhd_normalizes_statement_share_fields_as_shares() -> None:
    normalizer = EODHDFactsNormalizer(
        concepts=["EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding"]
    )
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "commonStockSharesOutstanding": 1500,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    shares = [
        r
        for r in records
        if r.concept == "CommonStockSharesOutstanding" and r.fiscal_period == "FY"
    ]
    entity = [
        r
        for r in records
        if r.concept == "EntityCommonStockSharesOutstanding" and r.fiscal_period == "FY"
    ]

    assert shares
    assert entity
    assert shares[0].unit_kind == "count"
    assert shares[0].currency is None
    assert entity[0].unit_kind == "count"
    assert entity[0].currency is None


def test_eodhd_normalizes_weighted_average_shares_as_count() -> None:
    """Weighted-average share counts are ``count`` facts, not monetary (regression).

    EODHD reports ``weightedAverageShsOutDil`` / ``weightedAverageShsOut`` on the
    income statement. They are share *quantities*, so they must normalize to
    ``unit_kind='count'`` with no currency -- not to a monetary value carrying the
    statement currency (which would let ``fcf_per_share_cagr_10y`` treat a share
    count as money).
    """

    normalizer = EODHDFactsNormalizer(
        concepts=[
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "WeightedAverageNumberOfSharesOutstandingBasic",
        ]
    )
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "weightedAverageShsOutDil": 1200,
                        "weightedAverageShsOut": 1000,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    diluted = [
        r
        for r in records
        if r.concept == "WeightedAverageNumberOfDilutedSharesOutstanding"
        and r.fiscal_period == "FY"
    ]
    basic = [
        r
        for r in records
        if r.concept == "WeightedAverageNumberOfSharesOutstandingBasic"
        and r.fiscal_period == "FY"
    ]

    assert diluted
    assert basic
    assert diluted[0].unit_kind == "count"
    assert diluted[0].currency is None
    assert diluted[0].value == 1200.0
    assert basic[0].unit_kind == "count"
    assert basic[0].currency is None
    assert basic[0].value == 1000.0


def test_eodhd_prefers_dedicated_outstanding_shares_over_scaled_statement_duplicate() -> (
    None
):
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2025-12-31",
                        "commonStockSharesOutstanding": 384_512_470_000.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "outstandingShares": {
            "annual": {
                "0": {
                    "date": "2025",
                    "dateFormatted": "2025-12-31",
                    "shares": 384_512_500,
                }
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    common_fy = [
        r
        for r in records
        if r.concept == "CommonStockSharesOutstanding"
        and r.fiscal_period == "FY"
        and r.end_date == "2025-12-31"
    ]

    assert len(common_fy) == 1
    assert common_fy[0].unit_kind == "count"
    assert common_fy[0].value == 384_512_500.0


def test_eodhd_derives_common_equity_from_stockholders_equity() -> None:
    normalizer = EODHDFactsNormalizer(concepts=["StockholdersEquity"])
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalStockholderEquity": 900.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "CommonStockholdersEquity"]
    assert derived, "CommonStockholdersEquity should be derived from StockholdersEquity"
    assert derived[0].value == 900.0


def test_eodhd_normalizes_net_income_to_common_from_applicable_shares() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "netIncomeApplicableToCommonShares": 120.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [
        r
        for r in records
        if r.concept == "NetIncomeLossAvailableToCommonStockholdersBasic"
    ]
    assert derived, (
        "NetIncomeLossAvailableToCommonStockholdersBasic should map from netIncomeApplicableToCommonShares"
    )
    assert derived[0].value == 120.0


def test_eodhd_normalizes_operating_income_from_ebit() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "ebit": 55.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "OperatingIncomeLoss"]
    assert derived, (
        "OperatingIncomeLoss should map from ebit when operatingIncome is missing"
    )
    assert derived[0].value == 55.0


def test_eodhd_normalizes_common_equity_from_common_stock_total_equity() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "commonStockTotalEquity": 500.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "CommonStockholdersEquity"]
    assert derived, "CommonStockholdersEquity should map from commonStockTotalEquity"
    assert derived[0].value == 500.0
    equity = [r for r in records if r.concept == "StockholdersEquity"]
    assert equity, "StockholdersEquity should fall back to CommonStockholdersEquity"
    assert equity[0].value == 500.0


def test_eodhd_normalizes_ebitda() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "ebitda": 42.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "EBITDA"]
    assert derived, "EBITDA should map from ebitda"
    assert derived[0].value == 42.0


def test_eodhd_normalizes_gross_profit() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "grossProfit": 420.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [r for r in records if r.concept == "GrossProfit"]
    assert derived, "GrossProfit should map from grossProfit"
    assert derived[0].value == 420.0


def test_eodhd_normalizes_cost_of_revenue() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "costOfRevenue": 580.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [r for r in records if r.concept == "CostOfRevenue"]
    assert derived, "CostOfRevenue should map from costOfRevenue"
    assert derived[0].value == 580.0


def test_eodhd_normalizes_common_stock_dividends_paid() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Cash_Flow": {
                "quarterly": [
                    {
                        "date": "2024-12-31",
                        "dividendsPaid": -25.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [r for r in records if r.concept == "CommonStockDividendsPaid"]
    assert derived, "CommonStockDividendsPaid should map from dividendsPaid"
    assert derived[0].value == -25.0


def test_eodhd_normalizes_dividend_share_from_highlights() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Highlights": {
            "DividendShare": 3.2,
            "MostRecentQuarter": "2024-09-30",
        },
        "General": {"CurrencyCode": "USD", "UpdatedAt": "2024-11-05"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [
        r for r in records if r.concept == "CommonStockDividendsPerShareCashPaid"
    ]
    assert derived, (
        "CommonStockDividendsPerShareCashPaid should map from Highlights.DividendShare"
    )
    assert derived[0].value == 3.2
    # DividendShare is a TTM scalar timestamped by General.UpdatedAt, NOT by the
    # balance-sheet quarter — see EODHD's Fundamentals Glossary.
    assert derived[0].fiscal_period == "TTM"
    assert derived[0].end_date == "2024-11-05"


def test_eodhd_dividend_share_skipped_when_updated_at_missing() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Highlights": {
            "DividendShare": 3.2,
            "MostRecentQuarter": "2024-09-30",
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [
        r for r in records if r.concept == "CommonStockDividendsPerShareCashPaid"
    ]
    assert not derived, (
        "DividendShare snapshot must be skipped when General.UpdatedAt is absent"
    )


def test_eodhd_normalizes_provider_market_cap_from_highlights() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Highlights": {"MarketCapitalization": 3_318_691_135_488.0},
        "General": {"CurrencyCode": "USD", "UpdatedAt": "2026-03-29"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    anchors = [r for r in records if r.concept == "ProviderMarketCapitalization"]
    assert anchors, (
        "ProviderMarketCapitalization should map from Highlights.MarketCapitalization"
    )
    anchor = anchors[0]
    assert anchor.value == 3_318_691_135_488.0
    # Like the SharesStats/DividendShare snapshots, the Highlights block is
    # refreshed as a unit and timestamped by General.UpdatedAt.
    assert anchor.fiscal_period == "INSTANT"
    assert anchor.end_date == "2026-03-29"
    assert anchor.unit_kind == "monetary"
    assert anchor.currency == "USD"


def test_eodhd_provider_market_cap_gbx_collapses_code_but_not_amount() -> None:
    # EODHD quotes Highlights.MarketCapitalization in MAJOR units even when
    # General.CurrencyCode is a subunit code (verified against stored GBX/ZAC
    # payloads), so the code collapses to GBP while the amount must NOT be
    # divided by the 100x subunit divisor.
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Highlights": {"MarketCapitalization": 3_007_000_000.0},
        "General": {"CurrencyCode": "GBX", "UpdatedAt": "2026-03-29"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    anchors = [r for r in records if r.concept == "ProviderMarketCapitalization"]
    assert anchors
    assert anchors[0].currency == "GBP"
    assert anchors[0].value == 3_007_000_000.0


def test_eodhd_provider_market_cap_skipped_when_absent_or_non_positive() -> None:
    normalizer = EODHDFactsNormalizer()
    for highlights in (
        None,
        {},
        {"MarketCapitalization": None},
        {"MarketCapitalization": "n/a"},
        {"MarketCapitalization": 0},
        {"MarketCapitalization": -5.0},
    ):
        payload: dict[str, object] = {
            "General": {"CurrencyCode": "USD", "UpdatedAt": "2026-03-29"}
        }
        if highlights is not None:
            payload["Highlights"] = highlights
        records = normalizer.normalize(payload, symbol="TEST.US")
        anchors = [r for r in records if r.concept == "ProviderMarketCapitalization"]
        assert not anchors, (
            f"placeholder provider cap {highlights!r} must not become a fact"
        )


def test_eodhd_provider_market_cap_skipped_when_updated_at_missing() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Highlights": {"MarketCapitalization": 1_000_000.0},
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    anchors = [r for r in records if r.concept == "ProviderMarketCapitalization"]
    assert not anchors, (
        "provider market-cap snapshot must be skipped when General.UpdatedAt is absent"
    )


def test_eodhd_normalizes_short_term_debt_and_cash_investments() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "shortTermDebt": 15.0,
                        "cashAndShortTermInvestments": 60.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    short_term = [r for r in records if r.concept == "ShortTermDebt"]
    cash = [r for r in records if r.concept == "CashAndShortTermInvestments"]
    assert short_term, "ShortTermDebt should map from shortTermDebt"
    assert cash, (
        "CashAndShortTermInvestments should map from cashAndShortTermInvestments"
    )
    assert short_term[0].value == 15.0
    assert cash[0].value == 60.0


def test_eodhd_normalizes_interest_expense() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "interestExpense": 12.5,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "InterestExpense"]
    assert derived, "InterestExpense should map from interestExpense"
    assert derived[0].value == 12.5


def test_eodhd_derives_interest_expense_from_net_interest_income() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "interestIncome": 40.0,
                        "netInterestIncome": 15.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [
        r for r in records if r.concept == "InterestExpenseFromNetInterestIncome"
    ]
    assert derived, "InterestExpenseFromNetInterestIncome should be derived"
    assert derived[0].value == 25.0


def test_eodhd_skips_non_positive_derived_interest_expense() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "interestIncome": 15.0,
                        "netInterestIncome": 15.0,
                        "currency_symbol": "USD",
                    },
                    {
                        "date": "2023-12-31",
                        "interestIncome": 10.0,
                        "netInterestIncome": 12.0,
                        "currency_symbol": "USD",
                    },
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [
        r for r in records if r.concept == "InterestExpenseFromNetInterestIncome"
    ]
    assert not derived


def test_eodhd_normalizes_income_tax_expense() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "incomeTaxExpense": 18.5,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "IncomeTaxExpense"]
    assert derived, "IncomeTaxExpense should map from incomeTaxExpense"
    assert derived[0].value == 18.5


def test_eodhd_normalizes_income_tax_expense_from_tax_provision() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "taxProvision": 22.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "IncomeTaxExpense"]
    assert derived, "IncomeTaxExpense should map from taxProvision fallback"
    assert derived[0].value == 22.0


def test_eodhd_does_not_normalize_long_term_debt_from_short_long_term_debt_total() -> (
    None
):
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "shortLongTermDebtTotal": 250.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "LongTermDebt"]
    assert not derived, "LongTermDebt should not map from shortLongTermDebtTotal"


def test_eodhd_normalizes_total_debt_from_short_long_term_debt_total() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "shortLongTermDebtTotal": 250.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "TotalDebtFromBalanceSheet"]
    assert derived
    assert derived[0].value == 250.0


def test_eodhd_skips_non_numeric_total_debt_from_short_long_term_debt_total() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "shortLongTermDebtTotal": "not-a-number",
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "TotalDebtFromBalanceSheet"]
    assert not derived


def test_eodhd_normalizes_da_from_income_statement() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "depreciationAndAmortization": 34.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [
        r for r in records if r.concept == "DepreciationDepletionAndAmortization"
    ]
    assert derived, (
        "DepreciationDepletionAndAmortization should map from depreciationAndAmortization"
    )
    assert derived[0].value == 34.0


def test_eodhd_normalizes_da_from_reconciled_depreciation_fallback() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "reconciledDepreciation": 21.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [
        r for r in records if r.concept == "DepreciationDepletionAndAmortization"
    ]
    assert derived, (
        "DepreciationDepletionAndAmortization should fall back to reconciledDepreciation"
    )
    assert derived[0].value == 21.0


def test_eodhd_normalizes_depreciation_from_cash_flow() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Cash_Flow": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "depreciation": 18.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "DepreciationFromCashFlow"]
    assert derived, "DepreciationFromCashFlow should map from cash-flow depreciation"
    assert derived[0].value == 18.0


def test_eodhd_normalizes_cash_and_cash_equivalents() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "cashAndEquivalents": 44.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "CashAndCashEquivalents"]
    assert derived, "CashAndCashEquivalents should map from cashAndEquivalents"
    assert derived[0].value == 44.0


def test_eodhd_normalizes_cash_and_cash_equivalents_fallback_from_cash() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "cash": 33.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "CashAndCashEquivalents"]
    assert derived, "CashAndCashEquivalents should fall back to cash"
    assert derived[0].value == 33.0


def test_eodhd_normalizes_short_term_investments() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "shortTermInvestments": 19.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "ShortTermInvestments"]
    assert derived, "ShortTermInvestments should map from shortTermInvestments"
    assert derived[0].value == 19.0


def test_eodhd_normalizes_sale_purchase_of_stock() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Cash_Flow": {
                "quarterly": [
                    {
                        "date": "2024-12-31",
                        "salePurchaseOfStock": -25.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "SalePurchaseOfStock"]
    assert derived, "SalePurchaseOfStock should map from salePurchaseOfStock"
    assert derived[0].value == -25.0
    assert derived[0].fiscal_period == "Q4"


def test_eodhd_normalizes_issuance_of_capital_stock() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Cash_Flow": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "issuanceOfCapitalStock": 12.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "IssuanceOfCapitalStock"]
    assert derived, "IssuanceOfCapitalStock should map from issuanceOfCapitalStock"
    assert derived[0].value == 12.0
    assert derived[0].fiscal_period == "FY"


def test_eodhd_normalizes_stock_based_compensation() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Cash_Flow": {
                "quarterly": [
                    {
                        "date": "2024-12-31",
                        "stockBasedCompensation": 18.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE")
    derived = [r for r in records if r.concept == "StockBasedCompensation"]
    assert derived, "StockBasedCompensation should map from stockBasedCompensation"
    assert derived[0].value == 18.0
    assert derived[0].fiscal_period == "Q4"


def test_eodhd_extract_value_reuses_case_insensitive_lookup() -> None:
    normalizer = EODHDFactsNormalizer()
    entry = {
        "totalAssets": "10.0",
        "TOTALLIABILITIES": "7.0",
    }
    lowered = normalizer._build_case_insensitive_entry(entry)

    assert normalizer._extract_value(entry, ["totalAssets"], lowered) == 10.0
    assert normalizer._extract_value(entry, ["totalLiabilities"], lowered) == 7.0


def test_eodhd_normalizes_mixed_case_statement_keys() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "TotalAssets": 250.0,
                        "TOTALCURRENTLIABILITIES": 80.0,
                        "totalLiabilities": 120.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    concepts = {record.concept: record.value for record in records}

    assert concepts["Assets"] == 250.0
    assert concepts["Liabilities"] == 120.0
    assert concepts["LiabilitiesCurrent"] == 80.0


def test_eodhd_common_stockholders_equity_override_replaces_base_record() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalStockholderEquity": 100.0,
                        "commonStockTotalEquity": 95.0,
                        "preferredStockTotalEquity": 10.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [r for r in records if r.concept == "CommonStockholdersEquity"]

    assert len(derived) == 1
    assert derived[0].value == 90.0


def test_eodhd_normalizes_eps_for_configured_subunit_family() -> None:
    normalizer = EODHDFactsNormalizer()
    payload = {
        "General": {"CurrencyCode": "ZAC"},
        "Earnings": {
            "History": {
                "2024-03-31": {"date": "2024-03-31", "epsActual": 250.0},
                "2024-06-30": {"date": "2024-06-30", "epsActual": 300.0},
            }
        },
    }

    records = normalizer.normalize(payload, symbol="ABG.JSE")
    eps_records = [r for r in records if r.concept == "EarningsPerShareDiluted"]

    assert len(eps_records) == 2
    assert {record.end_date: record.value for record in eps_records} == {
        "2024-03-31": 2.5,
        "2024-06-30": 3.0,
    }
    # Subunit (ZAC) collapses to the major currency (ZAR), and EPS is a per_share
    # monetary fact — never a bare currency code in unit_kind.
    assert {record.currency for record in eps_records} == {"ZAR"}
    assert {record.unit_kind for record in eps_records} == {"per_share"}


# ---------------------------------------------------------------------------
# target_currency conversion
# ---------------------------------------------------------------------------


def _fx_service_with_rates(tmp_path: Path, *records: FXRateRecord) -> FXService:
    db_path = tmp_path / "fx.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(list(records))
    return FXService(db_path, repository=repo, provider_name="EODHD", preload_all=True)


def test_eodhd_normalize_target_currency_identity() -> None:
    """When all facts are already in the target currency, values stay unchanged."""

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 1000.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US", target_currency="USD")
    assets = [r for r in records if r.concept == "Assets"]
    assert assets
    assert assets[0].value == 1000.0
    assert assets[0].currency == "USD"


def test_eodhd_normalize_uses_entry_currency_before_statement_and_payload() -> None:
    """Entry-level currency wins over statement and payload defaults."""

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "CurrencyCode": "EUR",
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 1000.0,
                        "currency_symbol": "USD",
                    }
                ],
            }
        },
        "General": {"CurrencyCode": "JPY"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    assets = [r for r in records if r.concept == "Assets"]
    assert assets
    assert assets[0].currency == "USD"


def test_eodhd_normalize_uses_statement_currency_before_payload() -> None:
    """Direct statement-level currency is used when the entry has no currency."""

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "currency_symbol": "EUR",
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 1000.0,
                    }
                ],
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU")
    assets = [r for r in records if r.concept == "Assets"]
    assert assets
    assert assets[0].currency == "EUR"


def test_eodhd_normalize_converts_monetary_facts_to_target_currency(
    tmp_path: Path,
) -> None:
    """EUR-denominated facts are converted to USD using the FX rate."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate=1.1,
            fetched_at="2024-12-31",
            source_kind="provider",
        ),
    )
    normalizer = EODHDFactsNormalizer(fx_service=fx)
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 1000.0,
                        "currency_symbol": "EUR",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "EUR"},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU", target_currency="USD")
    assets = [r for r in records if r.concept == "Assets"]
    assert assets
    assert assets[0].currency == "USD"
    assert assets[0].unit_kind == "monetary"
    assert abs(assets[0].value - 1100.0) < 0.01


def test_eodhd_normalize_shares_not_converted_to_target_currency(
    tmp_path: Path,
) -> None:
    """Share count facts pass through unchanged regardless of target_currency."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate=1.1,
            fetched_at="2024-12-31",
            source_kind="provider",
        ),
    )
    normalizer = EODHDFactsNormalizer(fx_service=fx)
    payload = {
        "SharesStats": {"SharesOutstanding": 5000000},
        "General": {"CurrencyCode": "EUR", "UpdatedAt": "2024-12-31"},
        "Financials": {},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU", target_currency="USD")
    shares = [
        r
        for r in records
        if r.concept == "CommonStockSharesOutstanding" and r.fiscal_period == "INSTANT"
    ]
    assert shares
    assert shares[0].currency is None
    assert shares[0].value == 5000000
    assert shares[0].end_date == "2024-12-31"


def test_eodhd_normalize_no_target_currency_preserves_source() -> None:
    """When target_currency is None, facts retain their source currency."""

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 1000.0,
                        "currency_symbol": "EUR",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "EUR"},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU", target_currency=None)
    assets = [r for r in records if r.concept == "Assets"]
    assert assets
    assert assets[0].currency == "EUR"
    assert assets[0].value == 1000.0


def test_eodhd_normalize_missing_fx_rate_drops_old_period_but_keeps_newer_converted_period(
    tmp_path: Path,
) -> None:
    """Old periods without FX are skipped while newer convertible periods survive."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2001-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate=1.25,
            fetched_at="2001-12-31",
            source_kind="provider",
        ),
    )
    normalizer = EODHDFactsNormalizer(fx_service=fx)
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2000-06-30",
                        "totalAssets": 1000.0,
                        "currency_symbol": "NLG",
                    },
                    {
                        "date": "2001-12-31",
                        "totalAssets": 1200.0,
                        "currency_symbol": "EUR",
                    },
                ]
            }
        },
        "General": {"CurrencyCode": "EUR"},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU", target_currency="USD")
    assets = sorted(
        [record for record in records if record.concept == "Assets"],
        key=lambda record: record.end_date,
    )

    assert [(record.end_date, record.currency, record.value) for record in assets] == [
        ("2001-12-31", "USD", 1500.0)
    ]


def test_eodhd_normalize_missing_fx_rate_skips_old_derived_period_during_target_alignment(
    tmp_path: Path,
) -> None:
    """Derived periods still drop individually when target-currency FX is missing."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2001-12-31",
            base_currency="USD",
            quote_currency="EUR",
            rate=0.8,
            fetched_at="2001-12-31",
            source_kind="provider",
        ),
    )
    normalizer = EODHDFactsNormalizer(fx_service=fx)
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2000-06-30",
                        "totalAssets": 1000.0,
                        "currency_symbol": "EUR",
                        "totalLiab": 600.0,
                        "currency": "NLG",
                    },
                    {
                        "date": "2001-12-31",
                        "totalAssets": 200.0,
                        "currency_symbol": "EUR",
                        "totalLiab": 50.0,
                        "currency": "USD",
                    },
                ]
            }
        },
        "General": {"CurrencyCode": "EUR"},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU", target_currency="EUR")
    equity = sorted(
        [
            record
            for record in records
            if record.concept == "StockholdersEquity" and record.fiscal_period == "FY"
        ],
        key=lambda record: record.end_date,
    )

    assert [(record.end_date, record.currency, record.value) for record in equity] == [
        ("2001-12-31", "EUR", 120.0)
    ]


def test_eodhd_normalize_gbx_subunit_with_gbp_target_no_double_conversion() -> None:
    """GBX currency code is normalized to GBP; values pass through without division.

    EODHD reports statement-level monetary values in the main currency even when
    currency_symbol is GBX, so the amount is NOT divided by 100.  The identity
    check in _convert_facts_to_target_currency confirms the fact is already in
    GBP and returns it unchanged.
    """

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 10000.0,
                        "currency_symbol": "GBX",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "GBX"},
    }

    records = normalizer.normalize(payload, symbol="TEST.LSE", target_currency="GBP")
    assets = [r for r in records if r.concept == "Assets"]
    assert assets
    # GBX code normalized to GBP; value NOT divided (statement amounts are main-unit)
    assert assets[0].currency == "GBP"
    assert assets[0].value == 10000.0
    # The subunit-quoted monetary fact lands as a major-currency monetary fact.
    assert assets[0].unit_kind == "monetary"


def test_eodhd_normalize_mixed_currencies_aligned_to_target(tmp_path: Path) -> None:
    """Facts with different source currencies are all aligned to the target."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate=1.1,
            fetched_at="2024-12-31",
            source_kind="provider",
        ),
    )
    normalizer = EODHDFactsNormalizer(fx_service=fx)
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 1000.0,
                        "currency_symbol": "USD",
                    }
                ]
            },
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalRevenue": 500.0,
                        "currency_symbol": "EUR",
                    }
                ]
            },
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US", target_currency="USD")
    assets = [r for r in records if r.concept == "Assets"]
    revenue = [r for r in records if r.concept == "Revenues"]
    assert assets and assets[0].currency == "USD"
    assert assets[0].value == 1000.0  # Already in USD, no conversion
    assert revenue and revenue[0].currency == "USD"
    assert abs(revenue[0].value - 550.0) < 0.01  # 500 EUR * 1.1


def test_eodhd_normalize_no_fx_service_returns_unconverted() -> None:
    """When no FX service is available, facts pass through in source currency."""

    normalizer = EODHDFactsNormalizer(fx_service=None)
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 1000.0,
                        "currency_symbol": "EUR",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "EUR"},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU", target_currency="USD")
    assets = [r for r in records if r.concept == "Assets"]
    # No FX service -> conversion step returns records unchanged
    assert assets
    assert assets[0].currency == "EUR"
    assert assets[0].value == 1000.0


def test_eodhd_preferred_stock_ignores_capital_stock() -> None:
    """capitalStock must not be read as PreferredStock.

    Regression: EODHD's ``capitalStock`` is common stock + additional paid-in
    capital for issuers without preferred shares, so it must never populate the
    PreferredStock concept. With no genuine preferred field present, no
    PreferredStock fact should be emitted.
    """

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "capitalStock": 200.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    preferred = [r for r in records if r.concept == "PreferredStock"]
    assert not preferred, "capitalStock must not be normalized as PreferredStock"


def test_eodhd_common_equity_nonnegative_without_preferred_stock() -> None:
    """Common equity must not be reduced by mislabeled capitalStock.

    Regression for the AAPL bug: with only ``capitalStock`` present and no real
    preferred stock, the derived CommonStockholdersEquity (StockholdersEquity -
    PreferredStock - NCI) must equal StockholdersEquity and stay non-negative.
    On the buggy code, capitalStock became a 200 PreferredStock fact and pushed
    CommonStockholdersEquity to 150 - 200 = -50.
    """

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalAssets": 250.0,
                        "totalLiab": 100.0,
                        "capitalStock": 200.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    equity = [r for r in records if r.concept == "StockholdersEquity"]
    common_equity = [r for r in records if r.concept == "CommonStockholdersEquity"]
    assert equity and equity[0].value == 150.0  # 250 assets - 100 liabilities
    assert common_equity, "CommonStockholdersEquity should be derived"
    assert common_equity[0].value == 150.0  # not reduced by common capitalStock
    assert common_equity[0].value >= 0
    assert not [r for r in records if r.concept == "PreferredStock"]


def test_eodhd_preferred_stock_read_from_genuine_field() -> None:
    """A real preferred-stock field still populates PreferredStock."""

    normalizer = EODHDFactsNormalizer()
    payload = {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "preferredStock": 40.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    preferred = [r for r in records if r.concept == "PreferredStock"]
    assert preferred, "PreferredStock should be read from a genuine preferred field"
    assert preferred[0].value == 40.0
    assert preferred[0].currency == "USD"
