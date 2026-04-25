# Relationships

The identity/catalog layer now uses physical foreign keys. Large downstream tables still mostly rely on application-maintained logical references because those tables are rebuilt and migrated carefully around the live SQLite database.

## Canonical Identity Flow

```mermaid
flowchart LR
    provider --> provider_exchange
    exchange --> provider_exchange
    issuer --> listing
    exchange --> listing
    provider --> provider_listing
    provider_exchange --> provider_listing
    listing --> provider_listing
    provider_listing --> fundamentals_fetch_state
    provider_listing --> fundamentals_raw
    provider_listing --> fundamentals_normalization_state
    provider_listing --> market_data_fetch_state
    listing --> financial_facts
    listing --> financial_facts_refresh_state
    listing --> market_data
    listing --> metrics
    listing --> metric_compute_status
```

## FX Flow

```mermaid
flowchart LR
    provider --> fx_supported_pairs
    provider --> fx_refresh_state
    provider --> fx_rates
    listing --> fx_rates
    financial_facts --> fx_rates
    fx_supported_pairs --> fx_refresh_state
    fx_refresh_state --> fx_rates
```

## Relationship Notes

- `provider.provider_id` is the catalog FK key; `provider.provider_code` remains the stable external namespace.
- `provider_exchange` maps provider exchange codes to canonical `exchange.exchange_id`.
- `listing.listing_id` is the canonical downstream key for facts, prices, metrics, and primary-listing status.
- `provider_listing.provider_listing_id` replaces `(provider, provider_symbol)` as the durable provider-scoped raw/state key.
- User-facing canonical symbols such as `AAPL.US` are derived from `listing.symbol` plus `exchange.exchange_code`.
- FX discovery reads currencies from `listing` and `financial_facts`, but FX
  storage itself is not keyed back to a listing.
