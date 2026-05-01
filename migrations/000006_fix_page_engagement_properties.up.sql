-- Drop existing buggy page_engagement objects (from 000002 and potentially unfixed 000005)

DROP VIEW IF EXISTS analytics.page_engagement_daily;
DROP VIEW IF EXISTS analytics.page_engagement_hourly;

DROP VIEW IF EXISTS analytics.mv_page_engagement_daily;
DROP VIEW IF EXISTS analytics.mv_page_engagement_hourly;

DROP TABLE IF EXISTS analytics.page_engagement_daily_state;
DROP TABLE IF EXISTS analytics.page_engagement_hourly_state;

-- Recreate daily with corrected schema (extracts 'duration' instead of 'time_on_page_ms', removes scroll_depth)

CREATE TABLE IF NOT EXISTS analytics.page_engagement_daily_state
(
    site_id String,
    day Date,
    url String,
    page_leaves SimpleAggregateFunction(sum, UInt64),
    avg_duration_ms AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(day), site_id)
ORDER BY (site_id, day, url);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_page_engagement_daily
TO analytics.page_engagement_daily_state
AS
SELECT
    site_id,
    toDate(timestamp) AS day,
    url,
    count() AS page_leaves,
    avgState(
        toFloat64(JSONExtractFloat(if(empty(properties), '{}', properties), 'duration'))
    ) AS avg_duration_ms
FROM analytics.events
WHERE event_type = 'page_leave'
GROUP BY site_id, day, url;

CREATE VIEW IF NOT EXISTS analytics.page_engagement_daily AS
SELECT
    site_id,
    day,
    url,
    sum(page_leaves) AS page_leaves,
    avgMerge(avg_duration_ms) AS avg_duration_ms
FROM analytics.page_engagement_daily_state
GROUP BY site_id, day, url;

-- Recreate hourly with corrected schema

CREATE TABLE IF NOT EXISTS analytics.page_engagement_hourly_state
(
    site_id String,
    hour DateTime,
    url String,
    page_leaves SimpleAggregateFunction(sum, UInt64),
    avg_duration_ms AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(hour), site_id)
ORDER BY (site_id, hour, url);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_page_engagement_hourly
TO analytics.page_engagement_hourly_state
AS
SELECT
    site_id,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS hour,
    url,
    count() AS page_leaves,
    avgState(
        toFloat64(JSONExtractFloat(if(empty(properties), '{}', properties), 'duration'))
    ) AS avg_duration_ms
FROM analytics.events
WHERE event_type = 'page_leave'
GROUP BY site_id, hour, url;

CREATE VIEW IF NOT EXISTS analytics.page_engagement_hourly AS
SELECT
    site_id,
    hour,
    url,
    sum(page_leaves) AS page_leaves,
    avgMerge(avg_duration_ms) AS avg_duration_ms
FROM analytics.page_engagement_hourly_state
GROUP BY site_id, hour, url;
