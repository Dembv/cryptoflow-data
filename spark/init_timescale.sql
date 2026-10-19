-- path: spark/init_timescale.sql

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── raw_trades (Binance tick data) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_trades (
    trade_id        BIGINT,
    symbol          TEXT        NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    side            TEXT,
    trade_time      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ,
    source          TEXT,
    PRIMARY KEY (symbol, trade_time, trade_id)
);
SELECT create_hypertable('raw_trades', 'trade_time', if_not_exists => TRUE);

-- ── prices_5min (continuous aggregate source) ───────────────────────────────
CREATE TABLE IF NOT EXISTS prices_5min (
    bucket          TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    trade_count     BIGINT,
    PRIMARY KEY (symbol, bucket)
);
SELECT create_hypertable('prices_5min', 'bucket', if_not_exists => TRUE);

-- ── indicators ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS indicators (
    computed_at     TIMESTAMPTZ NOT NULL,
    window_start    TIMESTAMPTZ,            -- fenêtre Spark d'origine (clé de déduplication)
    symbol          TEXT        NOT NULL,
    window_label    TEXT        NOT NULL,
    vwap            DOUBLE PRECISION,
    rsi_approx      DOUBLE PRECISION,
    bb_upper        DOUBLE PRECISION,
    bb_middle       DOUBLE PRECISION,
    bb_lower        DOUBLE PRECISION,
    bb_bandwidth    DOUBLE PRECISION,
    buy_ratio       DOUBLE PRECISION,
    trade_count     BIGINT,
    volume          DOUBLE PRECISION,
    PRIMARY KEY (symbol, window_label, computed_at)
);
SELECT create_hypertable('indicators', 'computed_at', if_not_exists => TRUE);

-- ── onchain_events ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS onchain_events (
    tx_hash         TEXT,
    event_type      TEXT        NOT NULL,
    symbol          TEXT        NOT NULL,
    block_number    BIGINT,
    timestamp_ts    TIMESTAMPTZ NOT NULL,
    from_address    TEXT,
    to_address      TEXT,
    value_eth       DOUBLE PRECISION,
    value_token     DOUBLE PRECISION,
    gas_used        BIGINT,
    ingested_at     TIMESTAMPTZ,
    PRIMARY KEY (tx_hash, timestamp_ts)
);
SELECT create_hypertable('onchain_events', 'timestamp_ts', if_not_exists => TRUE);

-- ── Index ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol    ON raw_trades (symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_indicators_symbol    ON indicators (symbol, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_onchain_symbol       ON onchain_events (symbol, timestamp_ts DESC);