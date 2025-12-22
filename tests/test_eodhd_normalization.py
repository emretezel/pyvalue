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
    derived = [r for r in records if r.concept == "IntangibleAssetsNetExcludingGoodwill"]
    assert derived, "IntangibleAssetsNetExcludingGoodwill should be derived from IntangibleAssetsNet"
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
    assert derived, "CommonStockSharesOutstanding should be derived from EntityCommonStockSharesOutstanding"
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
    derived = [r for r in records if r.concept == "NetIncomeLossAvailableToCommonStockholdersBasic"]
    assert derived, "NetIncomeLossAvailableToCommonStockholdersBasic should map from netIncomeApplicableToCommonShares"
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
    assert derived, "OperatingIncomeLoss should map from ebit when operatingIncome is missing"
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
