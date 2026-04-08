# Normalization and Facts

## Goal

Normalization converts provider-specific payloads into a shared concept model so metrics can read one consistent fact store.

## Fact Model

A normalized fact typically includes:
- symbol
- concept
- fiscal period
- end date
- value
- currency or unit metadata
- optional source metadata

Monetary and non-monetary facts are treated differently:

- monetary facts must resolve to a real currency code
- non-monetary facts keep a unit such as `shares`
- configured subunit currencies are converted to their base currencies before
  downstream arithmetic: `GBX`/`GBP0.01` -> `GBP`, `ZAC` -> `ZAR`, `ILA` -> `ILS`

## Provider-Agnostic Design

Metrics read from `financial_facts`, not directly from raw provider payloads.

That means:
- raw SEC and EODHD payloads can differ significantly
- normalization is where provider-specific mapping and fallback rules live
- metrics can stay focused on financial logic rather than source payload shape

For EODHD monetary fields, currency resolution follows one shared precedence:

1. row-level currency on the specific statement or earnings entry
2. statement-level currency
3. payload-level default currency
4. a narrow documented legacy fallback only when the fact `unit` already stores
   the ISO currency code

If a monetary field still cannot be assigned a currency, normalization logs a
warning and skips only that fact or derived fact.

## Practical Consequence

Two symbols can have the same metric logic but different provider-specific normalization paths underneath.

Examples:
- SEC and EODHD may expose different raw field names for cash flow, debt, or share counts
- EODHD-oriented metrics may rely on concepts or fallback chains that are realistically only available from EODHD normalization

Derived facts are also currency-aware:

- same-period accounting derivations prefer the statement/reporting currency for
  that period
- market-linked derivations prefer the market-data currency
- mixed-currency monetary inputs are converted before arithmetic
- missing currency skips only the affected derived fact and logs structured
  context
- missing FX is now a hard symbol-level error once normalization knows the
  source and target currencies but cannot resolve a stored direct, inverse, or
  USD/EUR triangulated quote

For bulk runs, each worker process preloads the full selected-provider FX table
once and resolves conversions from that local in-memory cache only. No runtime
FX web fetches happen during normalization.

## Normalization Layers in This Repo

- `src/pyvalue/normalization/`: provider-specific normalization logic
- `src/pyvalue/facts.py`: shared fact abstractions
- `src/pyvalue/storage.py`: persistence for normalized facts

## When to Update Normalization

Update normalization when:
- a needed source field is not mapped yet
- provider payload structure changes
- a new metric requires a new normalized concept
- fallback precedence should move from metric runtime into concept normalization

## When Not to Update Normalization

Do not add normalization just because a metric can combine existing normalized facts.

Prefer metric-level composition when:
- required concepts already exist
- the logic is metric-specific rather than canonical
- adding a new concept would only duplicate an existing calculation

## Related Docs

- [Data Model and Storage](data-model-and-storage.md)
- [EODHD Provider Guide](../providers/eodhd.md)
- [SEC Provider Guide](../providers/sec.md)
