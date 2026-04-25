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
            cik TEXT,
            concept TEXT NOT NULL,
            fiscal_period TEXT,
            end_date TEXT NOT NULL,
            unit TEXT NOT NULL,
            value REAL NOT NULL,
            accn TEXT,
            filed TEXT,
            frame TEXT,
            start_date TEXT,
            accounting_standard TEXT,
            currency TEXT,
            source_provider TEXT,
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit, accn)
        );
CREATE TABLE "financial_facts_refresh_state" (
            listing_id INTEGER NOT NULL PRIMARY KEY,
            refreshed_at TEXT NOT NULL
        );
CREATE TABLE "fundamentals_fetch_state" (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        );
CREATE TABLE "fundamentals_normalization_state" (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            listing_id INTEGER NOT NULL,
            raw_fetched_at TEXT NOT NULL,
            last_normalized_at TEXT NOT NULL,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        );
CREATE TABLE fundamentals_raw (
            payload_id INTEGER PRIMARY KEY,
            provider_listing_id INTEGER NOT NULL UNIQUE,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        );
CREATE TABLE fx_rates (
            provider TEXT NOT NULL,
            rate_date TEXT NOT NULL,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            rate_text TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            meta_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, rate_date, base_currency, quote_currency)
        );
CREATE TABLE fx_refresh_state (
            provider TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            min_rate_date TEXT,
            max_rate_date TEXT,
            full_history_backfilled INTEGER NOT NULL DEFAULT 0,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, canonical_symbol)
        );
CREATE TABLE fx_supported_pairs (
            provider TEXT NOT NULL,
            symbol TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            base_currency TEXT,
            quote_currency TEXT,
            name TEXT,
            is_alias INTEGER NOT NULL DEFAULT 0,
            is_refreshable INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (provider, symbol)
        );
CREATE TABLE issuer (
            issuer_id INTEGER PRIMARY KEY,
            name TEXT,
            description TEXT,
            sector TEXT,
            industry TEXT,
            country TEXT
        );
CREATE TABLE listing (
            listing_id INTEGER PRIMARY KEY,
            issuer_id INTEGER NOT NULL,
            exchange_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            currency TEXT, primary_listing_status TEXT NOT NULL DEFAULT 'unknown'
            CHECK (primary_listing_status IN ('unknown', 'primary', 'secondary')),
            UNIQUE (exchange_id, symbol),
            FOREIGN KEY (issuer_id) REFERENCES issuer(issuer_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        );
CREATE TABLE "market_data" (
            listing_id INTEGER NOT NULL,
            as_of DATE NOT NULL,
            price REAL NOT NULL,
            volume INTEGER,
            market_cap REAL,
            currency TEXT,
            source_provider TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (listing_id, as_of)
        );
CREATE TABLE "market_data_fetch_state" (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        );
CREATE TABLE "metric_compute_status" (
            listing_id INTEGER NOT NULL,
            metric_id TEXT NOT NULL,
            status TEXT NOT NULL,
            reason_code TEXT,
            reason_detail TEXT,
            attempted_at TEXT NOT NULL,
            value_as_of TEXT,
            facts_refreshed_at TEXT,
            market_data_as_of TEXT,
            market_data_updated_at TEXT,
            PRIMARY KEY (listing_id, metric_id)
        );
CREATE TABLE "metrics" (
            listing_id INTEGER NOT NULL,
            metric_id TEXT NOT NULL,
            value REAL NOT NULL,
            as_of TEXT NOT NULL,
            unit_kind TEXT NOT NULL DEFAULT 'other',
            currency TEXT,
            unit_label TEXT,
            PRIMARY KEY (listing_id, metric_id)
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
CREATE TABLE provider_exchange (
            provider_exchange_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            name TEXT,
            country TEXT,
            currency TEXT,
            operating_mic TEXT,
            country_iso2 TEXT,
            country_iso3 TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE (provider_id, provider_exchange_code),
            UNIQUE (provider_exchange_id, provider_id),
            FOREIGN KEY (provider_id) REFERENCES provider(provider_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        );
CREATE TABLE provider_listing (
            provider_listing_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            provider_exchange_id INTEGER NOT NULL,
            provider_symbol TEXT NOT NULL,
            currency TEXT,
            listing_id INTEGER NOT NULL,
            UNIQUE (provider_exchange_id, provider_symbol),
            FOREIGN KEY (provider_id) REFERENCES provider(provider_id),
            FOREIGN KEY (provider_exchange_id) REFERENCES provider_exchange(provider_exchange_id),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id),
            FOREIGN KEY (provider_exchange_id, provider_id)
                REFERENCES provider_exchange(provider_exchange_id, provider_id)
        );
CREATE TABLE schema_migrations (
            version INTEGER NOT NULL
        );
CREATE INDEX idx_fin_facts_concept
            ON financial_facts(concept);
CREATE INDEX idx_fin_facts_currency_nonnull
            ON financial_facts(currency)
            WHERE currency IS NOT NULL;
CREATE INDEX idx_fin_facts_security_concept
            ON financial_facts(listing_id, concept);
CREATE INDEX idx_fin_facts_security_concept_latest
            ON financial_facts(listing_id, concept, end_date DESC, filed DESC);
CREATE INDEX idx_fundamentals_fetch_next
            ON fundamentals_fetch_state(next_eligible_at);
CREATE INDEX idx_fundamentals_norm_state_security
            ON fundamentals_normalization_state(listing_id);
CREATE INDEX idx_fundamentals_raw_provider_fetched
        ON fundamentals_raw(fetched_at);
CREATE INDEX idx_fx_rates_pair_date
        ON fx_rates(provider, base_currency, quote_currency, rate_date DESC);
CREATE INDEX idx_fx_supported_pairs_refreshable
        ON fx_supported_pairs(provider, is_refreshable, canonical_symbol);
CREATE INDEX idx_listing_exchange
        ON listing(exchange_id);
CREATE INDEX idx_market_data_currency_nonnull
            ON market_data(currency)
            WHERE currency IS NOT NULL;
CREATE INDEX idx_market_data_fetch_next
            ON market_data_fetch_state(next_eligible_at);
CREATE INDEX idx_market_data_latest
            ON market_data(listing_id, as_of DESC);
CREATE INDEX idx_metric_compute_status_metric_status
            ON metric_compute_status(metric_id, status);
CREATE INDEX idx_metrics_metric_id
            ON metrics(metric_id);
CREATE INDEX idx_provider_exchange_exchange
        ON provider_exchange(exchange_id);
CREATE INDEX idx_provider_listing_currency_nonnull
        ON provider_listing(currency)
        WHERE currency IS NOT NULL;
CREATE INDEX idx_provider_listing_listing
        ON provider_listing(listing_id);
CREATE INDEX idx_provider_listing_provider
        ON provider_listing(provider_id);
