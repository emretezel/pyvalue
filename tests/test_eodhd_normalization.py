from pyvalue.fx import FXService
from pyvalue.normalization.eodhd import EODHDFactsNormalizer
from pyvalue.storage import FXRateRecord, FXRatesRepository


def test_eodhd_normalizes_ppe_net():
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


def test_eodhd_normalizes_assets_from_total_assets():
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


def test_eodhd_derives_intangibles_excluding_goodwill_from_net():
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


def test_eodhd_derives_common_shares_from_entity_shares():
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


def test_eodhd_normalizes_statement_share_fields_as_shares():
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
    assert shares[0].unit == "shares"
    assert shares[0].currency is None
    assert entity[0].unit == "shares"
    assert entity[0].currency is None


def test_eodhd_prefers_dedicated_outstanding_shares_over_scaled_statement_duplicate():
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
    assert common_fy[0].unit == "shares"
    assert common_fy[0].value == 384_512_500.0


def test_eodhd_derives_common_equity_from_stockholders_equity():
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


def test_eodhd_normalizes_net_income_to_common_from_applicable_shares():
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


def test_eodhd_normalizes_operating_income_from_ebit():
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


def test_eodhd_normalizes_common_equity_from_common_stock_total_equity():
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


def test_eodhd_normalizes_ebitda():
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


def test_eodhd_normalizes_gross_profit():
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


def test_eodhd_normalizes_cost_of_revenue():
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


def test_eodhd_normalizes_common_stock_dividends_paid():
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


def test_eodhd_normalizes_dividend_share_from_highlights():
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
    assert derived, (
        "CommonStockDividendsPerShareCashPaid should map from Highlights.DividendShare"
    )
    assert derived[0].value == 3.2
    assert derived[0].end_date == "2024-09-30"


def test_eodhd_normalizes_short_term_debt_and_cash_investments():
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


def test_eodhd_normalizes_interest_expense():
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


def test_eodhd_derives_interest_expense_from_net_interest_income():
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


def test_eodhd_skips_non_positive_derived_interest_expense():
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


def test_eodhd_normalizes_income_tax_expense():
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


def test_eodhd_normalizes_income_tax_expense_from_tax_provision():
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


def test_eodhd_does_not_normalize_long_term_debt_from_short_long_term_debt_total():
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


def test_eodhd_normalizes_total_debt_from_short_long_term_debt_total():
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


def test_eodhd_skips_non_numeric_total_debt_from_short_long_term_debt_total():
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


def test_eodhd_normalizes_da_from_income_statement():
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


def test_eodhd_normalizes_da_from_reconciled_depreciation_fallback():
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


def test_eodhd_normalizes_depreciation_from_cash_flow():
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


def test_eodhd_normalizes_cash_and_cash_equivalents():
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


def test_eodhd_normalizes_cash_and_cash_equivalents_fallback_from_cash():
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


def test_eodhd_normalizes_short_term_investments():
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


def test_eodhd_normalizes_enterprise_value_from_valuation():
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Valuation": {"EnterpriseValue": 1234.0},
        "Highlights": {"MostRecentQuarter": "2025-09-30"},
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [r for r in records if r.concept == "EnterpriseValue"]
    assert derived, "EnterpriseValue should map from Valuation.EnterpriseValue"
    assert derived[0].value == 1234.0
    assert derived[0].currency == "USD"
    assert derived[0].end_date == "2025-09-30"
    assert derived[0].fiscal_period == ""


def test_eodhd_enterprise_value_uses_most_recent_quarter_date_when_present():
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Valuation": {"EnterpriseValue": 555.0},
        "Highlights": {"MostRecentQuarter": "2025-06-30"},
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalRevenue": 1000.0,
                        "currency_symbol": "USD",
                    }
                ],
                "quarterly": [
                    {
                        "date": "2025-09-30",
                        "totalRevenue": 300.0,
                        "currency_symbol": "USD",
                    }
                ],
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [r for r in records if r.concept == "EnterpriseValue"]
    assert derived, "EnterpriseValue should be normalized when valuation value exists"
    assert derived[0].end_date == "2025-06-30"


def test_eodhd_enterprise_value_falls_back_to_latest_statement_date():
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Valuation": {"EnterpriseValue": 777.0},
        "Financials": {
            "Income_Statement": {
                "yearly": [
                    {
                        "date": "2024-12-31",
                        "totalRevenue": 1000.0,
                        "currency_symbol": "USD",
                    }
                ],
                "quarterly": [
                    {
                        "date": "2025-09-30",
                        "totalRevenue": 300.0,
                        "currency_symbol": "USD",
                    }
                ],
            }
        },
        "General": {"CurrencyCode": "USD"},
    }

    records = normalizer.normalize(payload, symbol="TEST.US")
    derived = [r for r in records if r.concept == "EnterpriseValue"]
    assert derived, "EnterpriseValue should fall back to latest statement date"
    assert derived[0].end_date == "2025-09-30"


def test_eodhd_enterprise_value_uses_statement_currency_when_general_missing():
    normalizer = EODHDFactsNormalizer()
    payload = {
        "Valuation": {"EnterpriseValue": 777.0},
        "Financials": {
            "Income_Statement": {
                "CurrencyCode": "USD",
                "quarterly": [
                    {
                        "date": "2025-09-30",
                        "totalRevenue": 300.0,
                    }
                ],
            }
        },
        "General": {"CurrencyCode": None},
    }

    records = normalizer.normalize(payload, symbol="TEST.BA")
    derived = [r for r in records if r.concept == "EnterpriseValue"]
    assert derived, "EnterpriseValue should use statement-derived currency"
    assert derived[0].currency == "USD"


def test_eodhd_normalizes_sale_purchase_of_stock():
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


def test_eodhd_normalizes_issuance_of_capital_stock():
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


def test_eodhd_normalizes_stock_based_compensation():
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


def test_eodhd_extract_value_reuses_case_insensitive_lookup():
    normalizer = EODHDFactsNormalizer()
    entry = {
        "totalAssets": "10.0",
        "TOTALLIABILITIES": "7.0",
    }
    lowered = normalizer._build_case_insensitive_entry(entry)

    assert normalizer._extract_value(entry, ["totalAssets"], lowered) == 10.0
    assert normalizer._extract_value(entry, ["totalLiabilities"], lowered) == 7.0


def test_eodhd_normalizes_mixed_case_statement_keys():
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


def test_eodhd_common_stockholders_equity_override_replaces_base_record():
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


def test_eodhd_normalizes_eps_for_configured_subunit_family():
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
    assert {record.currency for record in eps_records} == {"ZAR"}


# ---------------------------------------------------------------------------
# target_currency conversion
# ---------------------------------------------------------------------------


def _fx_service_with_rates(tmp_path, *records):
    db_path = tmp_path / "fx.db"
    repo = FXRatesRepository(db_path)
    repo.initialize_schema()
    repo.upsert_many(list(records))
    return FXService(db_path, repository=repo, provider_name="EODHD", preload_all=True)


def test_eodhd_normalize_target_currency_identity():
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


def test_eodhd_normalize_uses_entry_currency_before_statement_and_payload():
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


def test_eodhd_normalize_uses_statement_currency_before_payload():
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


def test_eodhd_normalize_converts_monetary_facts_to_target_currency(tmp_path):
    """EUR-denominated facts are converted to USD using the FX rate."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate_text="1.1",
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
    assert assets[0].unit == "USD"
    assert abs(assets[0].value - 1100.0) < 0.01


def test_eodhd_normalize_shares_not_converted_to_target_currency(tmp_path):
    """Share count facts pass through unchanged regardless of target_currency."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate_text="1.1",
            fetched_at="2024-12-31",
            source_kind="provider",
        ),
    )
    normalizer = EODHDFactsNormalizer(fx_service=fx)
    payload = {
        "SharesStats": {"SharesOutstanding": 5000000},
        "General": {"CurrencyCode": "EUR", "LatestQuarter": "2024-12-31"},
        "Financials": {},
    }

    records = normalizer.normalize(payload, symbol="TEST.EU", target_currency="USD")
    shares = [r for r in records if r.concept == "CommonStockSharesOutstanding"]
    assert shares
    assert shares[0].currency is None
    assert shares[0].value == 5000000


def test_eodhd_normalize_no_target_currency_preserves_source():
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
    tmp_path,
):
    """Old periods without FX are skipped while newer convertible periods survive."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2001-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate_text="1.25",
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
    tmp_path,
):
    """Derived periods still drop individually when target-currency FX is missing."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2001-12-31",
            base_currency="USD",
            quote_currency="EUR",
            rate_text="0.8",
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


def test_eodhd_normalize_gbx_subunit_with_gbp_target_no_double_conversion():
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


def test_eodhd_normalize_mixed_currencies_aligned_to_target(tmp_path):
    """Facts with different source currencies are all aligned to the target."""

    fx = _fx_service_with_rates(
        tmp_path,
        FXRateRecord(
            provider="EODHD",
            rate_date="2024-12-31",
            base_currency="EUR",
            quote_currency="USD",
            rate_text="1.1",
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


def test_eodhd_normalize_no_fx_service_returns_unconverted():
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
