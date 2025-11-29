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
