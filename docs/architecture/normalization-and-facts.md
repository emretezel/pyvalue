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

## Provider-Agnostic Design

Metrics read from `financial_facts`, not directly from raw provider payloads.

That means:
- raw SEC and EODHD payloads can differ significantly
- normalization is where provider-specific mapping and fallback rules live
- metrics can stay focused on financial logic rather than source payload shape

## Practical Consequence

Two symbols can have the same metric logic but different provider-specific normalization paths underneath.

Examples:
- SEC and EODHD may expose different raw field names for cash flow, debt, or share counts
- EODHD-oriented metrics may rely on concepts or fallback chains that are realistically only available from EODHD normalization

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
