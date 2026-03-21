from pyvalue.normalization.eodhd import EODHDFactsNormalizer


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
