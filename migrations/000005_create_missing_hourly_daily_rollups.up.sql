-- Daily counterparts for existing hourly rollups

CREATE TABLE IF NOT EXISTS analytics.site_metrics_daily_state
(
    site_id String,
    day Date,
    page_views SimpleAggregateFunction(sum, UInt64),
    unique_visitors AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(day), site_id)
ORDER BY (site_id, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_site_metrics_daily
TO analytics.site_metrics_daily_state
AS
SELECT
    site_id,
    toDate(timestamp) AS day,
    count() AS page_views,
    uniqState(visitor_id) AS unique_visitors,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type = 'page_view'
GROUP BY site_id, day;

CREATE VIEW IF NOT EXISTS analytics.site_metrics_daily AS
SELECT
    site_id,
    day,
    sum(page_views) AS page_views,
    uniqMerge(unique_visitors) AS unique_visitors,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.site_metrics_daily_state
GROUP BY site_id, day;

CREATE TABLE IF NOT EXISTS analytics.page_views_daily_state
(
    site_id String,
    day Date,
    url String,
    referrer String,
    country LowCardinality(String),
    region LowCardinality(String),
    device_type LowCardinality(String),
    browser LowCardinality(String),
    os LowCardinality(String),
    utm_source LowCardinality(String),
    utm_medium LowCardinality(String),
    utm_campaign LowCardinality(String),
    views SimpleAggregateFunction(sum, UInt64),
    unique_visitors AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(day), site_id)
ORDER BY
(
    site_id,
    day,
    url,
    referrer,
    country,
    region,
    device_type,
    browser,
    os,
    utm_source,
    utm_medium,
    utm_campaign
);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_page_views_daily
TO analytics.page_views_daily_state
AS
SELECT
    site_id,
    toDate(timestamp) AS day,
    url,
    if(empty(referrer), 'direct', referrer) AS referrer,
    if(empty(country), 'unknown', country) AS country,
    if(empty(region), 'unknown', region) AS region,
    if(empty(device_type), 'unknown', device_type) AS device_type,
    if(empty(browser), 'unknown', browser) AS browser,
    if(empty(os), 'unknown', os) AS os,
    if(empty(utm_source), 'none', utm_source) AS utm_source,
    if(empty(utm_medium), 'none', utm_medium) AS utm_medium,
    if(empty(utm_campaign), 'none', utm_campaign) AS utm_campaign,
    count() AS views,
    uniqState(visitor_id) AS unique_visitors,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type = 'page_view'
GROUP BY
    site_id,
    day,
    url,
    referrer,
    country,
    region,
    device_type,
    browser,
    os,
    utm_source,
    utm_medium,
    utm_campaign;

CREATE VIEW IF NOT EXISTS analytics.page_views_daily AS
SELECT
    site_id,
    day,
    url,
    referrer,
    country,
    region,
    device_type,
    browser,
    os,
    utm_source,
    utm_medium,
    utm_campaign,
    sum(views) AS views,
    uniqMerge(unique_visitors) AS unique_visitors,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.page_views_daily_state
GROUP BY
    site_id,
    day,
    url,
    referrer,
    country,
    region,
    device_type,
    browser,
    os,
    utm_source,
    utm_medium,
    utm_campaign;

-- Hourly counterparts for existing daily rollups

CREATE TABLE IF NOT EXISTS analytics.referrers_hourly_state
(
    site_id String,
    hour DateTime,
    referrer String,
    views SimpleAggregateFunction(sum, UInt64),
    unique_visitors AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(hour), site_id)
ORDER BY (site_id, hour, referrer);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_referrers_hourly
TO analytics.referrers_hourly_state
AS
SELECT
    site_id,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS hour,
    if(empty(referrer), 'direct', referrer) AS referrer,
    count() AS views,
    uniqState(visitor_id) AS unique_visitors,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type = 'page_view'
GROUP BY site_id, hour, referrer;

CREATE VIEW IF NOT EXISTS analytics.referrers_hourly AS
SELECT
    site_id,
    hour,
    referrer,
    sum(views) AS views,
    uniqMerge(unique_visitors) AS unique_visitors,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.referrers_hourly_state
GROUP BY site_id, hour, referrer;

CREATE TABLE IF NOT EXISTS analytics.country_breakdown_hourly_state
(
    site_id String,
    hour DateTime,
    country LowCardinality(String),
    region LowCardinality(String),
    page_views SimpleAggregateFunction(sum, UInt64),
    unique_visitors AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(hour), site_id)
ORDER BY (site_id, hour, country, region);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_country_breakdown_hourly
TO analytics.country_breakdown_hourly_state
AS
SELECT
    site_id,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS hour,
    if(empty(country), 'unknown', country) AS country,
    if(empty(region), 'unknown', region) AS region,
    count() AS page_views,
    uniqState(visitor_id) AS unique_visitors,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type = 'page_view'
GROUP BY site_id, hour, country, region;

CREATE VIEW IF NOT EXISTS analytics.country_breakdown_hourly AS
SELECT
    site_id,
    hour,
    country,
    region,
    sum(page_views) AS page_views,
    uniqMerge(unique_visitors) AS unique_visitors,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.country_breakdown_hourly_state
GROUP BY site_id, hour, country, region;

CREATE TABLE IF NOT EXISTS analytics.device_breakdown_hourly_state
(
    site_id String,
    hour DateTime,
    device_type LowCardinality(String),
    browser LowCardinality(String),
    os LowCardinality(String),
    page_views SimpleAggregateFunction(sum, UInt64),
    unique_visitors AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(hour), site_id)
ORDER BY (site_id, hour, device_type, browser, os);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_device_breakdown_hourly
TO analytics.device_breakdown_hourly_state
AS
SELECT
    site_id,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS hour,
    if(empty(device_type), 'unknown', device_type) AS device_type,
    if(empty(browser), 'unknown', browser) AS browser,
    if(empty(os), 'unknown', os) AS os,
    count() AS page_views,
    uniqState(visitor_id) AS unique_visitors,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type = 'page_view'
GROUP BY site_id, hour, device_type, browser, os;

CREATE VIEW IF NOT EXISTS analytics.device_breakdown_hourly AS
SELECT
    site_id,
    hour,
    device_type,
    browser,
    os,
    sum(page_views) AS page_views,
    uniqMerge(unique_visitors) AS unique_visitors,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.device_breakdown_hourly_state
GROUP BY site_id, hour, device_type, browser, os;

CREATE TABLE IF NOT EXISTS analytics.campaigns_hourly_state
(
    site_id String,
    hour DateTime,
    utm_source LowCardinality(String),
    utm_medium LowCardinality(String),
    utm_campaign LowCardinality(String),
    page_views SimpleAggregateFunction(sum, UInt64),
    unique_visitors AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(hour), site_id)
ORDER BY (site_id, hour, utm_source, utm_medium, utm_campaign);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_campaigns_hourly
TO analytics.campaigns_hourly_state
AS
SELECT
    site_id,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS hour,
    if(empty(utm_source), 'none', utm_source) AS utm_source,
    if(empty(utm_medium), 'none', utm_medium) AS utm_medium,
    if(empty(utm_campaign), 'none', utm_campaign) AS utm_campaign,
    count() AS page_views,
    uniqState(visitor_id) AS unique_visitors,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type = 'page_view'
GROUP BY site_id, hour, utm_source, utm_medium, utm_campaign;

CREATE VIEW IF NOT EXISTS analytics.campaigns_hourly AS
SELECT
    site_id,
    hour,
    utm_source,
    utm_medium,
    utm_campaign,
    sum(page_views) AS page_views,
    uniqMerge(unique_visitors) AS unique_visitors,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.campaigns_hourly_state
GROUP BY site_id, hour, utm_source, utm_medium, utm_campaign;

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
