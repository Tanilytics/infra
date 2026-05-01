CREATE TABLE IF NOT EXISTS analytics.media_engagement_hourly_state
(
    site_id String,
    hour DateTime,
    url String,
    video_id String,
    provider LowCardinality(String),
    plays SimpleAggregateFunction(sum, UInt64),
    completes SimpleAggregateFunction(sum, UInt64),
    pauses SimpleAggregateFunction(sum, UInt64),
    seeks SimpleAggregateFunction(sum, UInt64),
    buffers SimpleAggregateFunction(sum, UInt64),
    progress_events SimpleAggregateFunction(sum, UInt64),
    avg_duration_seconds AggregateFunction(avg, Float64),
    unique_viewers AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(hour), site_id)
ORDER BY (site_id, hour, url, video_id, provider);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_media_engagement_hourly
TO analytics.media_engagement_hourly_state
AS
SELECT
    site_id,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS hour,
    url,
    if(empty(JSONExtractString(if(empty(properties), '{}', properties), 'video_id')), 'unknown', JSONExtractString(if(empty(properties), '{}', properties), 'video_id')) AS video_id,
    if(empty(JSONExtractString(if(empty(properties), '{}', properties), 'provider')), 'unknown', JSONExtractString(if(empty(properties), '{}', properties), 'provider')) AS provider,
    countIf(event_type = 'media_play') AS plays,
    countIf(event_type = 'media_complete') AS completes,
    countIf(event_type = 'media_pause') AS pauses,
    countIf(event_type = 'media_seek') AS seeks,
    countIf(event_type = 'media_buffer') AS buffers,
    countIf(event_type = 'media_progress') AS progress_events,
    avgState(toFloat64(JSONExtractFloat(if(empty(properties), '{}', properties), 'duration'))) AS avg_duration_seconds,
    uniqState(visitor_id) AS unique_viewers,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type IN ('media_play', 'media_pause', 'media_seek', 'media_progress', 'media_buffer', 'media_complete')
GROUP BY site_id, hour, url, video_id, provider;

CREATE VIEW IF NOT EXISTS analytics.media_engagement_hourly AS
SELECT
    site_id,
    hour,
    url,
    video_id,
    provider,
    sum(plays) AS plays,
    sum(completes) AS completes,
    sum(pauses) AS pauses,
    sum(seeks) AS seeks,
    sum(buffers) AS buffers,
    sum(progress_events) AS progress_events,
    avgMerge(avg_duration_seconds) AS avg_duration_seconds,
    uniqMerge(unique_viewers) AS unique_viewers,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.media_engagement_hourly_state
GROUP BY site_id, hour, url, video_id, provider;

CREATE TABLE IF NOT EXISTS analytics.media_engagement_daily_state
(
    site_id String,
    day Date,
    url String,
    video_id String,
    provider LowCardinality(String),
    plays SimpleAggregateFunction(sum, UInt64),
    completes SimpleAggregateFunction(sum, UInt64),
    pauses SimpleAggregateFunction(sum, UInt64),
    seeks SimpleAggregateFunction(sum, UInt64),
    buffers SimpleAggregateFunction(sum, UInt64),
    progress_events SimpleAggregateFunction(sum, UInt64),
    avg_duration_seconds AggregateFunction(avg, Float64),
    unique_viewers AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY (toYYYYMM(day), site_id)
ORDER BY (site_id, day, url, video_id, provider);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_media_engagement_daily
TO analytics.media_engagement_daily_state
AS
SELECT
    site_id,
    toDate(timestamp) AS day,
    url,
    if(empty(JSONExtractString(if(empty(properties), '{}', properties), 'video_id')), 'unknown', JSONExtractString(if(empty(properties), '{}', properties), 'video_id')) AS video_id,
    if(empty(JSONExtractString(if(empty(properties), '{}', properties), 'provider')), 'unknown', JSONExtractString(if(empty(properties), '{}', properties), 'provider')) AS provider,
    countIf(event_type = 'media_play') AS plays,
    countIf(event_type = 'media_complete') AS completes,
    countIf(event_type = 'media_pause') AS pauses,
    countIf(event_type = 'media_seek') AS seeks,
    countIf(event_type = 'media_buffer') AS buffers,
    countIf(event_type = 'media_progress') AS progress_events,
    avgState(toFloat64(JSONExtractFloat(if(empty(properties), '{}', properties), 'duration'))) AS avg_duration_seconds,
    uniqState(visitor_id) AS unique_viewers,
    uniqState(session_id) AS unique_sessions
FROM analytics.events
WHERE event_type IN ('media_play', 'media_pause', 'media_seek', 'media_progress', 'media_buffer', 'media_complete')
GROUP BY site_id, day, url, video_id, provider;

CREATE VIEW IF NOT EXISTS analytics.media_engagement_daily AS
SELECT
    site_id,
    day,
    url,
    video_id,
    provider,
    sum(plays) AS plays,
    sum(completes) AS completes,
    sum(pauses) AS pauses,
    sum(seeks) AS seeks,
    sum(buffers) AS buffers,
    sum(progress_events) AS progress_events,
    avgMerge(avg_duration_seconds) AS avg_duration_seconds,
    uniqMerge(unique_viewers) AS unique_viewers,
    uniqMerge(unique_sessions) AS unique_sessions
FROM analytics.media_engagement_daily_state
GROUP BY site_id, day, url, video_id, provider;
