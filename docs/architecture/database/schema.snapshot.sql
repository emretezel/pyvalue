CREATE TABLE schema_migrations (
            version INTEGER NOT NULL
        );
CREATE TABLE securities (
            security_id INTEGER PRIMARY KEY,
            canonical_ticker TEXT NOT NULL,
            canonical_exchange_code TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            entity_name TEXT,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, sector TEXT, industry TEXT,
            UNIQUE (canonical_exchange_code, canonical_ticker),
            UNIQUE (canonical_symbol)
        );
CREATE INDEX idx_securities_exchange
        ON securities(canonical_exchange_code)
        ;
CREATE TABLE supported_tickers (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            provider_ticker TEXT NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            security_id INTEGER NOT NULL,
            listing_exchange TEXT,
            security_name TEXT,
            security_type TEXT,
            country TEXT,
            currency TEXT,
            isin TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_symbol)
        );
CREATE UNIQUE INDEX idx_supported_tickers_provider_exchange_ticker
        ON supported_tickers(provider, provider_exchange_code, provider_ticker)
        ;
CREATE INDEX idx_supported_tickers_provider_exchange
        ON supported_tickers(provider, provider_exchange_code)
        ;
CREATE INDEX idx_supported_tickers_security
        ON supported_tickers(security_id)
        ;
CREATE TABLE fundamentals_raw (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            security_id INTEGER NOT NULL,
            provider_exchange_code TEXT,
            currency TEXT,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_symbol)
        );
CREATE INDEX idx_fundamentals_raw_security
        ON fundamentals_raw(security_id)
        ;
CREATE INDEX idx_fundamentals_raw_provider_fetched
        ON fundamentals_raw(provider, fetched_at)
        ;
CREATE TABLE fundamentals_fetch_state (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, provider_symbol)
        );
CREATE INDEX idx_fundamentals_fetch_next
        ON fundamentals_fetch_state(provider, next_eligible_at)
        ;
CREATE TABLE market_data_fetch_state (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, provider_symbol)
        );
CREATE INDEX idx_market_data_fetch_next
        ON market_data_fetch_state(provider, next_eligible_at)
        ;
CREATE TABLE financial_facts (
            security_id INTEGER NOT NULL,
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
            PRIMARY KEY (security_id, concept, fiscal_period, end_date, unit, accn)
        );
CREATE INDEX idx_fin_facts_security_concept
        ON financial_facts(security_id, concept)
        ;
CREATE INDEX idx_fin_facts_concept
        ON financial_facts(concept)
        ;
CREATE TABLE market_data (
            security_id INTEGER NOT NULL,
            as_of DATE NOT NULL,
            price REAL NOT NULL,
            volume INTEGER,
            market_cap REAL,
            currency TEXT,
            source_provider TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (security_id, as_of)
        );
CREATE INDEX idx_market_data_latest
        ON market_data(security_id, as_of DESC)
        ;
CREATE TABLE metrics (
            security_id INTEGER NOT NULL,
            metric_id TEXT NOT NULL,
            value REAL NOT NULL,
            as_of TEXT NOT NULL, unit_kind TEXT NOT NULL DEFAULT 'other', currency TEXT, unit_label TEXT,
            PRIMARY KEY (security_id, metric_id)
        );
CREATE INDEX idx_metrics_metric_id
        ON metrics(metric_id)
        ;
CREATE INDEX idx_fundamentals_fetch_state_provider_fetched_symbol
        ON fundamentals_fetch_state(provider, last_fetched_at, provider_symbol)
        ;
CREATE INDEX idx_fundamentals_fetch_state_provider_status_next_symbol
        ON fundamentals_fetch_state(provider, last_status, next_eligible_at, provider_symbol)
        ;
CREATE INDEX idx_fin_facts_security_concept_latest
                ON financial_facts(security_id, concept, end_date DESC, filed DESC)
                ;
CREATE TABLE fundamentals_normalization_state (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            security_id INTEGER NOT NULL,
            raw_fetched_at TEXT NOT NULL,
            last_normalized_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_symbol)
        );
CREATE INDEX idx_fundamentals_norm_state_security
        ON fundamentals_normalization_state(security_id)
        ;
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
CREATE INDEX idx_fx_rates_pair_date
        ON fx_rates(provider, base_currency, quote_currency, rate_date DESC)
        ;
CREATE INDEX idx_supported_tickers_currency_nonnull
            ON supported_tickers(currency)
            WHERE currency IS NOT NULL
            ;
CREATE INDEX idx_fin_facts_currency_nonnull
            ON financial_facts(currency)
            WHERE currency IS NOT NULL
            ;
CREATE INDEX idx_market_data_currency_nonnull
            ON market_data(currency)
            WHERE currency IS NOT NULL
            ;
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
CREATE INDEX idx_fx_supported_pairs_refreshable
        ON fx_supported_pairs(provider, is_refreshable, canonical_symbol)
        ;
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
CREATE TABLE financial_facts_refresh_state (
            security_id INTEGER NOT NULL PRIMARY KEY,
            refreshed_at TEXT NOT NULL
        );
CREATE TABLE metric_compute_status (
            security_id INTEGER NOT NULL,
            metric_id TEXT NOT NULL,
            status TEXT NOT NULL,
            reason_code TEXT,
            reason_detail TEXT,
            attempted_at TEXT NOT NULL,
            value_as_of TEXT,
            facts_refreshed_at TEXT,
            market_data_as_of TEXT,
            market_data_updated_at TEXT,
            PRIMARY KEY (security_id, metric_id)
        );
CREATE INDEX idx_metric_compute_status_metric_status
        ON metric_compute_status(metric_id, status)
        ;
CREATE TABLE security_listing_status (
            security_id INTEGER NOT NULL PRIMARY KEY,
            source_provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            raw_fetched_at TEXT NOT NULL,
            is_primary_listing INTEGER NOT NULL CHECK (is_primary_listing IN (0, 1)),
            primary_provider_symbol TEXT,
            classification_basis TEXT NOT NULL CHECK (
                classification_basis IN (
                    'matched_primary_ticker',
                    'different_primary_ticker',
                    'missing_primary_ticker'
                )
            ),
            updated_at TEXT NOT NULL
        );
CREATE INDEX idx_security_listing_status_primary
        ON security_listing_status(is_primary_listing, security_id)
        ;
CREATE TABLE providers (
            provider_code TEXT NOT NULL PRIMARY KEY CHECK (
                provider_code = UPPER(TRIM(provider_code))
                AND LENGTH(TRIM(provider_code)) > 0
            ),
            display_name TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'active' CHECK (
                status IN ('active', 'deprecated', 'disabled')
            ),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
CREATE TABLE IF NOT EXISTS "exchange" (
            exchange_id INTEGER PRIMARY KEY,
            exchange_code TEXT NOT NULL UNIQUE CHECK (
                exchange_code = UPPER(TRIM(exchange_code))
                AND LENGTH(TRIM(exchange_code)) > 0
            ),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
CREATE TABLE exchange_provider (
            provider TEXT NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            name TEXT,
            country TEXT,
            currency TEXT,
            operating_mic TEXT,
            country_iso2 TEXT,
            country_iso3 TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_exchange_code),
            FOREIGN KEY (provider) REFERENCES providers(provider_code),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        );
CREATE INDEX idx_exchange_provider_exchange
        ON exchange_provider(exchange_id)
        ;
