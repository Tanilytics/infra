CREATE TABLE IF NOT EXISTS default.events
(
    event_id UUID,
    site_id String,
    visitor_id String,
    session_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    url String,
    referrer String,
    utm_source String,
    utm_medium String,
    utm_campaign String,
    country LowCardinality(String),
    region LowCardinality(String),
    device_type LowCardinality(String),
    browser LowCardinality(String),
    os LowCardinality(String),
    screen_width UInt16,
    properties String,
    ip_hash String,
    consent_given Bool
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (site_id, timestamp, visitor_id, event_id);
