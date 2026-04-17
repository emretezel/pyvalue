# Code Map

## Core entry points

- `src/pyvalue/cli.py`
  - `cmd_report_screen_failures()`: drives the screen-level audit and recomputes
    missing metrics on demand
  - `_recompute_missing_screen_metrics()`: records root-cause buckets for
    screen-missing metrics
  - `cmd_report_metric_failures()`: metric-level reason buckets
  - `cmd_report_fact_freshness()`: concept-level missing vs stale counts
  - `_format_failure_reason()`: turns warning output into the stored reason code
- `src/pyvalue/reporting.py`
  - `compute_fact_coverage()`: concept freshness accounting used by
    `report-fact-freshness`
- `src/pyvalue/storage.py`
  - `FundamentalsRepository`: raw `fundamentals_raw`
  - `FinancialFactsRepository`: normalized `financial_facts`
  - `MarketDataRepository`: latest market-cap snapshots
  - table definitions: `supported_tickers`, `securities`, `market_data`,
    `fundamentals_raw`, `financial_facts`
- `src/pyvalue/normalization/eodhd.py`
  - `EODHDFactsNormalizer`
  - `EODHD_STATEMENT_FIELDS`: canonical concept-to-EODHD field mapping
  - `_normalize_enterprise_value()`
  - `_normalize_share_counts()`
  - `_normalize_outstanding_shares()`
  - `_normalize_dividends_per_share()`
  - EPS implied-fallback helpers
- `src/pyvalue/metrics/utils.py`
  - `MAX_FACT_AGE_DAYS`
  - `MAX_FY_FACT_AGE_DAYS`
  - `is_recent_fact()`
- `src/pyvalue/metrics/__init__.py`
  - `REGISTRY`: metric id to class lookup

## What to inspect for each failure class

### Report says data is missing

- Check the metric module for `required_concepts` and any metric-specific
  fallback helpers.
- Check `references/code-map.md` and `src/pyvalue/normalization/eodhd.py` to
  see whether the raw payload should have produced the missing concept.
- If the raw payload has the field but normalized facts do not, suspect
  normalization.

### Report says too few facts exist

- Inspect the metric module for the real horizon rule: 4 quarters, 5 FY points,
  7 FY points, 10 FY points, and so on.
- Use the probe output to see how many FY and quarterly facts the sampled names
  actually have.
- Decide whether a shorter-horizon variant would still preserve the intended
  value or quality signal.

### Report says the latest fact is stale

- Check the exact freshness gate in the metric module or shared helpers.
- Compare the latest normalized fact date with the latest raw payload dates.
- If raw data is fresher than normalized facts, treat that as a normalization
  or raw-field-selection issue before relaxing thresholds.

## Existing commands to reuse

### Re-run the screen failure report

```bash
conda run -n pyvalue pyvalue report-screen-failures \
  --database data/pyvalue.db \
  --config screeners/value.yml \
  --exchange-codes LSE
```

### Re-run the metric failure report

```bash
conda run -n pyvalue pyvalue report-metric-failures \
  --database data/pyvalue.db \
  --metrics opm_10y_min \
  --exchange-codes LSE
```

### Check concept freshness for the same metric

```bash
conda run -n pyvalue pyvalue report-fact-freshness \
  --database data/pyvalue.db \
  --metrics opm_10y_min \
  --exchange-codes LSE \
  --show-all
```

### Run the bounded probe

```bash
conda run -n pyvalue python \
  skills/pyvalue-screen-failure-audit/scripts/metric_failure_probe.py \
  --database data/pyvalue.db \
  --metric-id opm_10y_min \
  --reason "missing FY EBIT history" \
  --exchange-codes LSE
```

## Read-only sampling query

Use this query pattern when the script is not enough and you need to inspect the
sample logic manually. Keep it exchange-scoped and capped.

```sql
WITH scope AS (
    SELECT DISTINCT
        st.security_id,
        st.provider_exchange_code AS exchange_code,
        s.canonical_symbol,
        COALESCE(s.entity_name, st.security_name) AS entity_name
    FROM supported_tickers st
    JOIN securities s ON s.security_id = st.security_id
    WHERE st.provider = 'EODHD'
      AND st.provider_exchange_code IN ('LSE')
),
latest AS (
    SELECT
        md.security_id,
        md.as_of,
        md.market_cap,
        md.currency,
        ROW_NUMBER() OVER (
            PARTITION BY md.security_id
            ORDER BY md.as_of DESC
        ) AS rn
    FROM market_data md
    JOIN scope ON scope.security_id = md.security_id
),
ranked AS (
    SELECT
        scope.exchange_code,
        scope.canonical_symbol,
        scope.entity_name,
        latest.as_of,
        latest.market_cap,
        latest.currency,
        ROW_NUMBER() OVER (
            PARTITION BY scope.exchange_code
            ORDER BY latest.market_cap DESC, scope.canonical_symbol
        ) AS exchange_rank
    FROM scope
    JOIN latest ON latest.security_id = scope.security_id
    WHERE latest.rn = 1
      AND latest.market_cap IS NOT NULL
)
SELECT *
FROM ranked
WHERE exchange_rank <= 50
ORDER BY exchange_code, exchange_rank;
```

## Interpreting probe output

- `fresh_latest`, `stale_latest`, `missing_latest`
  - latest normalized-fact status using the selected freshness window
- `symbols_with_any_raw`
  - how many sampled names have at least one candidate raw field for the
    concept
- `symbols_raw_but_no_normalized`
  - strongest normalization-bug signal
- `median_normalized_fy_count` and `median_normalized_quarter_count`
  - quick view of whether a long-horizon metric is failing because the history
    is not there
- `source_hints`
  - grep-like lines from the metric source that mention fallback or freshness

## Market-cap anomaly rule

- If one of the sampled market caps looks implausibly large for the company or
  exchange, compare it against current public information on the web.
- Use exact dates in the report:
  - latest stored `market_data.as_of`
  - the date of the public market-cap source you checked
- Treat the anomaly as evidence, not as proof of the metric bug, until the
  upstream field and currency path are understood.
