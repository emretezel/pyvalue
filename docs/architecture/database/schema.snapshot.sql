CREATE TABLE "exchange" (
            exchange_id INTEGER PRIMARY KEY,
            exchange_code TEXT NOT NULL UNIQUE CHECK (
                exchange_code = UPPER(TRIM(exchange_code))
                AND LENGTH(TRIM(exchange_code)) > 0
            ),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
CREATE TABLE "financial_facts" (
            listing_id INTEGER NOT NULL,
            concept TEXT NOT NULL,
            fiscal_period TEXT NOT NULL
                CHECK (fiscal_period IN ('FY','Q1','Q2','Q3','Q4','TTM','INSTANT')),
            end_date TEXT NOT NULL,
            unit_kind TEXT NOT NULL
                CHECK (unit_kind IN (
                    'monetary','per_share','ratio','percent','multiple','count','other'
                )),
            value REAL NOT NULL,
            filed TEXT,
            currency TEXT
                CHECK (
                    (currency IS NULL OR (length(currency) = 3 AND currency = upper(currency) AND currency GLOB '[A-Z][A-Z][A-Z]' AND currency NOT IN ('GBX', 'GBP0.01', 'ZAC', 'ILA')))
                    AND (
                        (unit_kind IN ('monetary','per_share') AND currency IS NOT NULL)
                        OR (unit_kind NOT IN ('monetary','per_share') AND currency IS NULL)
                    )
                ),
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        );
CREATE TABLE "financial_facts_refresh_state" (
            listing_id INTEGER NOT NULL PRIMARY KEY,
            refreshed_at TEXT NOT NULL,
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        );
CREATE TABLE fundamentals_fetch_state (
                provider_listing_id INTEGER PRIMARY KEY,
                failed_at TEXT NOT NULL,
                error TEXT NOT NULL,
                next_eligible_at TEXT NOT NULL,
                attempts INTEGER NOT NULL CHECK (attempts > 0),
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            );
CREATE TABLE fundamentals_normalization_state (
                provider_listing_id INTEGER PRIMARY KEY,
                normalized_payload_hash TEXT NOT NULL CHECK (length(normalized_payload_hash) = 64),
                normalized_at TEXT NOT NULL,
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            );
CREATE TABLE fundamentals_raw (
                provider_listing_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
                last_fetched_at TEXT NOT NULL,
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            );
CREATE TABLE "fx_rates" (
            provider TEXT NOT NULL,
            rate_date TEXT NOT NULL,
            base_currency TEXT NOT NULL CHECK (length(base_currency) = 3 AND base_currency = upper(base_currency) AND base_currency GLOB '[A-Z][A-Z][A-Z]'),
            quote_currency TEXT NOT NULL CHECK (length(quote_currency) = 3 AND quote_currency = upper(quote_currency) AND quote_currency GLOB '[A-Z][A-Z][A-Z]'),
            rate REAL NOT NULL,
            fetched_at TEXT NOT NULL,
            source_kind TEXT NOT NULL
                CHECK (source_kind IN ('provider')),
            meta_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (
                provider, rate_date, base_currency, quote_currency
            ),
            FOREIGN KEY (provider) REFERENCES provider(provider_code)
        );
CREATE TABLE "fx_refresh_state" (
                    provider TEXT NOT NULL,
                    canonical_symbol TEXT NOT NULL,
                    min_rate_date TEXT,
                    max_rate_date TEXT,
                    full_history_backfilled INTEGER NOT NULL DEFAULT 0
                        CHECK (full_history_backfilled IN (0, 1)),
                    last_fetched_at TEXT,
                    last_status TEXT
                        CHECK (last_status IS NULL
                               OR last_status IN ('ok', 'error')),
                    last_error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0
                        CHECK (attempts >= 0),
                    PRIMARY KEY (provider, canonical_symbol),
            FOREIGN KEY (provider) REFERENCES provider(provider_code)
                );
CREATE TABLE "fx_supported_pairs" (
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    canonical_symbol TEXT NOT NULL,
                    base_currency TEXT,
                    quote_currency TEXT,
                    name TEXT,
                    is_alias INTEGER NOT NULL DEFAULT 0
                        CHECK (is_alias IN (0, 1)),
                    is_refreshable INTEGER NOT NULL DEFAULT 0
                        CHECK (is_refreshable IN (0, 1)),
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (provider, symbol),
            FOREIGN KEY (provider) REFERENCES provider(provider_code)
                );
CREATE TABLE "issuer" (
            issuer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            sector TEXT,
            industry TEXT,
            country TEXT
        );
CREATE TABLE "listing" (
            listing_id INTEGER PRIMARY KEY,
            issuer_id INTEGER NOT NULL,
            exchange_id INTEGER NOT NULL,
            symbol TEXT NOT NULL
                CHECK (length(symbol) > 0
                       AND symbol = upper(trim(symbol))
                       AND instr(symbol, ' ') = 0
                       AND symbol GLOB '[A-Z0-9.&^*-]*'),
            currency TEXT NOT NULL
                CHECK (length(currency) = 3 AND currency = upper(currency) AND currency GLOB '[A-Z][A-Z][A-Z]'),
            primary_listing_status TEXT NOT NULL DEFAULT 'unknown',
            UNIQUE (exchange_id, symbol),
            FOREIGN KEY (issuer_id) REFERENCES issuer(issuer_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        );
CREATE TABLE "market_data" (
            listing_id INTEGER NOT NULL,
            as_of DATE NOT NULL,
            price REAL NOT NULL,
            volume INTEGER,
            source_provider TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (listing_id, as_of),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        );
CREATE TABLE "market_data_fetch_state" (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            last_fetched_at TEXT,
            last_status TEXT
                CHECK (last_status IS NULL
                       OR last_status IN ('ok', 'error')),
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0
                CHECK (attempts >= 0),
            CHECK (last_status != 'error' OR last_error IS NOT NULL),
            FOREIGN KEY (provider_listing_id)
                REFERENCES provider_listing(provider_listing_id)
        );
CREATE TABLE "metric_compute_status" (
                    listing_id INTEGER NOT NULL,
                    metric_id TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK (status IN ('success', 'failure')),
                    reason_code TEXT,
                    reason_detail TEXT,
                    attempted_at TEXT NOT NULL,
                    value_as_of TEXT,
                    facts_refreshed_at TEXT,
                    market_data_as_of TEXT,
                    market_data_updated_at TEXT,
                    PRIMARY KEY (listing_id, metric_id),
                    FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
                );
CREATE TABLE "metrics" (
                listing_id INTEGER NOT NULL,
                metric_id TEXT NOT NULL,
                value REAL NOT NULL,
                as_of TEXT NOT NULL,
                unit_kind TEXT NOT NULL DEFAULT 'other',
                currency TEXT,
                unit_label TEXT,
                PRIMARY KEY (listing_id, metric_id),
                FOREIGN KEY (listing_id) REFERENCES listing(listing_id),
                CHECK (
                    unit_kind IN (
                        'monetary', 'per_share', 'ratio', 'percent',
                        'multiple', 'count', 'other'
                    )
                ),
                CHECK (
                    currency IS NULL
                    OR unit_kind IN ('monetary', 'per_share')
                )
            );
CREATE TABLE provider (
            provider_id INTEGER PRIMARY KEY,
            provider_code TEXT NOT NULL UNIQUE CHECK (
                provider_code = UPPER(TRIM(provider_code))
                AND LENGTH(TRIM(provider_code)) > 0
            ),
            display_name TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
CREATE TABLE "provider_exchange" (
            provider_exchange_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            country TEXT NOT NULL,
            currency TEXT
                CHECK (currency IS NULL OR (length(currency) = 3 AND currency = upper(currency) AND currency GLOB '[A-Z][A-Z][A-Z]')),
            operating_mic TEXT,
            country_iso2 TEXT,
            country_iso3 TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE (provider_id, provider_exchange_code),
            UNIQUE (provider_exchange_id, provider_id),
            FOREIGN KEY (provider_id) REFERENCES provider(provider_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        );
CREATE TABLE "provider_listing" (
            provider_listing_id INTEGER PRIMARY KEY,
            provider_exchange_id INTEGER NOT NULL,
            provider_symbol TEXT NOT NULL,
            listing_id INTEGER NOT NULL,
            UNIQUE (provider_exchange_id, provider_symbol),
            FOREIGN KEY (provider_exchange_id)
                REFERENCES provider_exchange(provider_exchange_id),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        );
CREATE TABLE "schema_migrations" (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL
        );
CREATE INDEX idx_fin_facts_currency_nonnull
        ON financial_facts(currency)
        WHERE currency IS NOT NULL;
CREATE INDEX idx_fin_facts_security_concept_latest
        ON financial_facts(listing_id, concept, end_date DESC, filed DESC);
CREATE INDEX idx_fundamentals_raw_last_fetched
        ON fundamentals_raw(last_fetched_at);
CREATE INDEX idx_fx_rates_pair_date
        ON fx_rates(provider, base_currency, quote_currency, rate_date DESC);
CREATE INDEX idx_fx_supported_pairs_refreshable
                ON fx_supported_pairs(provider, is_refreshable, canonical_symbol);
CREATE UNIQUE INDEX idx_issuer_name_country
        ON issuer(name, country);
CREATE INDEX idx_listing_issuer
        ON listing(issuer_id);
CREATE INDEX idx_provider_listing_listing
        ON provider_listing(listing_id);
