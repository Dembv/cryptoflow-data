-- path: spark/init_timescale_policies.sql
-- Bougies 5 min réelles (depuis raw_trades), compression et rétention.

CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', trade_time) AS bucket,
    symbol,
    first(price, trade_time) AS open,
    max(price)               AS high,
    min(price)               AS low,
    last(price, trade_time)  AS close,
    sum(quantity)            AS volume,
    count(*)                 AS trade_count
FROM raw_trades
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_5m',
    start_offset      => INTERVAL '2 hours',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE);

ALTER TABLE raw_trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'trade_time DESC');

SELECT add_compression_policy('raw_trades', INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_retention_policy('raw_trades',   INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('indicators',   INTERVAL '90 days', if_not_exists => TRUE);

-- ── Migration idempotente (volumes existants) ───────────────────────────────
-- Fenêtre Spark d'origine : clé de déduplication des indicateurs (Spark ré-émet
-- chaque fenêtre à chaque trigger ; on ne garde que la dernière version à la lecture).
ALTER TABLE indicators ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ;
UPDATE indicators SET window_start = computed_at WHERE window_start IS NULL;
CREATE INDEX IF NOT EXISTS idx_indicators_window
    ON indicators (symbol, window_label, window_start DESC, computed_at DESC);

-- ── Bougies 5 min historiques (backfill Binance) + vue unifiée ──────────────
CREATE TABLE IF NOT EXISTS klines_5m (
    bucket          TIMESTAMPTZ      NOT NULL,
    symbol          TEXT             NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    trade_count     BIGINT,
    PRIMARY KEY (symbol, bucket)
);
SELECT create_hypertable('klines_5m', 'bucket', if_not_exists => TRUE);

-- Temps réel (ohlcv_5m) prioritaire ; l'historique Binance comble le reste.
CREATE OR REPLACE VIEW candles_5m AS
    SELECT bucket, symbol, open, high, low, close, volume, trade_count FROM ohlcv_5m
    UNION ALL
    SELECT k.bucket, k.symbol, k.open, k.high, k.low, k.close, k.volume, k.trade_count
    FROM klines_5m k
    WHERE NOT EXISTS (
        SELECT 1 FROM ohlcv_5m o WHERE o.symbol = k.symbol AND o.bucket = k.bucket
    );
