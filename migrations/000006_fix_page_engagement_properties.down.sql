-- Rollback: restore the original buggy page_engagement schema

DROP VIEW IF EXISTS analytics.page_engagement_daily;
DROP VIEW IF EXISTS analytics.page_engagement_hourly;

DROP VIEW IF EXISTS analytics.mv_page_engagement_daily;
DROP VIEW IF EXISTS analytics.mv_page_engagement_hourly;

DROP TABLE IF EXISTS analytics.page_engagement_daily_state;
DROP TABLE IF EXISTS analytics.page_engagement_hourly_state;

-- Recreate daily with original buggy schema

CREATE TABLE IF NOT EXISTS analytics.page_engagement_daily_state
(
    site_id String,
    day Date,
    url String,
    page_leaves SimpleAggregateFunction(sum, UInt64),
    avg_time_on_page_ms AggregateFunction(avg, Float64),
    avg_scroll_depth_pct AggregateFunction(avg, Float64)
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
        toFloat64(JSONExtractUInt(if(empty(properties), '{}', properties), 'time_on_page_ms'))
    ) AS avg_time_on_page_ms,
    avgState(
        toFloat64(JSONExtractFloat(if(empty(properties), '{}', properties), 'scroll_depth_pct'))
    ) AS avg_scroll_depth_pct
FROM analytics.events
WHERE event_type = 'page_leave'
GROUP BY site_id, day, url;

CREATE VIEW IF NOT EXISTS analytics.page_engagement_daily AS
SELECT
    site_id,
    day,
    url,
    sum(page_leaves) AS page_leaves,
    avgMerge(avg_time_on_page_ms) AS avg_time_on_page_ms,
    avgMerge(avg_scroll_depth_pct) AS avg_scroll_depth_pct
FROM analytics.page_engagement_daily_state
GROUP BY site_id, day, url;

-- Recreate hourly with original buggy schema

CREATE TABLE IF NOT EXISTS analytics.page_engagement_hourly_state
(
    site_id String,
    hour DateTime,
    url String,
    page_leaves SimpleAggregateFunction(sum, UInt64),
    avg_time_on_page_ms AggregateFunction(avg, Float64),
    avg_scroll_depth_pct AggregateFunction(avg, Float64)
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
        toFloat64(JSONExtractUInt(if(empty(properties), '{}', properties), 'time_on_page_ms'))
    ) AS avg_time_on_page_ms,
    avgState(
        toFloat64(JSONExtractFloat(if(empty(properties), '{}', properties), 'scroll_depth_pct'))
    ) AS avg_scroll_depth_pct
FROM analytics.events
WHERE event_type = 'page_leave'
GROUP BY site_id, hour, url;

CREATE VIEW IF NOT EXISTS analytics.page_engagement_hourly AS
SELECT
    site_id,
    hour,
    url,
    sum(page_leaves) AS page_leaves,
    avgMerge(avg_time_on_page_ms) AS avg_time_on_page_ms,
    avgMerge(avg_scroll_depth_pct) AS avg_scroll_depth_pct
FROM analytics.page_engagement_hourly_state
GROUP BY site_id, hour, url;
