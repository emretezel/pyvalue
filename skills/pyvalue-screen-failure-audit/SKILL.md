---
name: pyvalue-screen-failure-audit
description: Investigate `pyvalue report-screen-failures` and `report-metric-failures` root-cause buckets for this repo using EODHD data only. Use when Codex needs to validate whether a reported missing, stale, or insufficient-history metric failure is actually caused by missing raw fundamentals, a normalization error, missing fallback fundamentals, or an overly tight staleness window; always keep the audit bounded to the 50 largest-market-cap tickers per exchange in scope and compare suspicious market-cap outliers against the web.
---

# Pyvalue Screen Failure Audit

## Overview

Audit one metric failure reason from `pyvalue report-screen-failures` or
`report-metric-failures` with bounded evidence. Start from the reported metric
and reason, then trace the claim back through EODHD raw payloads, normalized
facts, metric code, and existing fallback logic.

## Guardrails

- Use `EODHD` only. Ignore SEC and other providers unless the user explicitly
  changes the scope.
- Reuse the original exchange scope from the failure report. If the user does
  not provide the exchange scope, ask for it instead of defaulting to the full
  database.
- Never run temp scripts or ad hoc full-db scans. Keep every audit bounded to
  the 50 largest-market-cap tickers per exchange in scope.
- Run project commands in the `pyvalue` conda environment.
- Treat market-cap anomalies as data-quality clues. If a sampled market cap
  looks implausibly large, compare it with current public information on the web
  and report the anomaly with exact dates.
- Check sign and currency sanity while auditing. For UK symbols, explicitly
  guard against `GBP` and `GBX` mixups.

## Workflow

### 1. Lock the failure target

- Capture the exact metric id and reported reason string from
  `report-screen-failures` or `report-metric-failures`.
- Capture the exchange scope and screen config if the failure came from
  `report-screen-failures`.
- Load the metric class from `pyvalue.metrics.REGISTRY` and note:
  - `required_concepts`
  - source file
  - any obvious fallback helpers
  - any freshness checks such as `is_recent_fact`, `MAX_FACT_AGE_DAYS`, or
    `MAX_FY_FACT_AGE_DAYS`

### 2. Gather bounded evidence

- Run the bundled probe first:

```bash
conda run -n pyvalue python \
  skills/pyvalue-screen-failure-audit/scripts/metric_failure_probe.py \
  --database data/pyvalue.db \
  --metric-id opm_10y_min \
  --reason "missing FY EBIT history" \
  --exchange-codes LSE
```

- Use the probe output to identify:
  - the sampled symbols and their latest market caps
  - which required concepts are missing, stale, or present in normalized facts
  - whether the raw EODHD payload already contains candidate fields for those
    concepts
  - which symbols have raw data but no normalized fact
- If the failure still looks ambiguous, run the existing CLI reports on the same
  scope:

```bash
conda run -n pyvalue pyvalue report-fact-freshness \
  --database data/pyvalue.db \
  --metrics opm_10y_min \
  --exchange-codes LSE \
  --show-all
```

### 3. Validate the claimed root cause

- Treat the reported reason as a hypothesis, not a conclusion.
- If the raw payload lacks the needed field on the sampled large-cap names, the
  missing-data diagnosis is probably real.
- If the raw payload contains the field but the normalized fact is absent or far
  older than the raw payload suggests, treat that as a likely normalization
  problem and inspect `src/pyvalue/normalization/eodhd.py`.
- If the normalized fact exists with suspicious magnitude or sign, inspect the
  raw value, currency, unit normalization, and any metric-side arithmetic before
  accepting the reason.

### 4. Check fallback fundamentals

- Inspect both the metric module and `src/pyvalue/normalization/eodhd.py` for
  existing fallback behavior before proposing anything new.
- Prefer fallback facts that:
  - already exist in the EODHD payload
  - preserve the economic meaning of the metric
  - materially improve coverage across the sampled large-cap names
- Reject fallback ideas that silently change the metric into something else.
- If a fallback would help, state whether it belongs in normalization,
  metric-side fallback logic, or both.

### 5. Evaluate fewer-than-N variants

- Determine the real horizon requirement from the metric code, not only from
  `required_concepts`.
- If a metric fails because it has fewer than `N` usable facts, ask whether a
  shorter-horizon version would still screen value or quality stocks sensibly.
- Accept a shorter-horizon variant only when the financial meaning survives.
  Examples:
  - a 7Y variant of a 10Y stability metric may be defensible if it still spans
    multiple cycles
  - replacing a long-horizon durability metric with a 3Y version is usually too
    weak
- When proposing a variant, say clearly whether it should replace the existing
  metric, live alongside it, or stay out of the default screen.

### 6. Evaluate stale-data thresholds

- Find the exact freshness gate in the metric path.
- Treat more than one year old as suspicious by default. Smaller relaxations can
  be reasonable if the metric still reflects current business quality.
- Prefer a fresher fallback fundamental over relaxing the staleness threshold.
- If the raw payload contains a fresher candidate fact that normalization does
  not currently surface, favor that fix over a looser age rule.

### 7. Report the audit cleanly

- Report in this order:
  - metric, reported reason, and exchange scope
  - sample basis: 50 largest market caps per exchange, EODHD only
  - verdict on whether the reported root cause is actually correct
  - raw-vs-normalized evidence
  - fallback opportunities
  - whether a shorter-horizon variant is financially defensible
  - whether the staleness threshold is too tight
  - any market-cap anomalies checked against the web
  - concrete code changes if the audit points to a bug or a worthwhile fallback

## References

- Read [code-map.md](references/code-map.md) for the exact repo files, helper
  commands, and SQL/query patterns behind this workflow.
