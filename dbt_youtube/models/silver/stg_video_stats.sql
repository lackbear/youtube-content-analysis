{{ config(materialized='table', tags=['silver']) }}

-- Cleaned, typed silver staging of bronze video_stats.
--
-- Grain: one row per (video_id, ingestion_timestamp) — the snapshot history
-- is preserved (NOT deduplicated to "latest only") so downstream gold
-- models can compute deltas across snapshots.
--
-- Filters: only `status = 'clean'` rows are kept. Missing/private/deleted
-- videos surface in bronze as `status = 'missing'` with empty stats; they
-- carry no analytical value and are dropped here.

with bronze as (
    select * from {{ source('bronze', 'video_stats') }}
)

-- nullif(...,''):: pattern: YouTube returns empty strings (not zeros) when
-- like/comment counts are hidden by the channel or disabled on the video.
-- The bare cast(...as bigint) raises on those rows. Treating them as NULL
-- is correct semantically (we don't know the count) and gold's arithmetic
-- propagates NULL cleanly.
select
    video_id,
    channel_id,
    channel_name,
    title,
    nullif(published_at, '')::timestamp        as published_at,
    nullif(views,    '')::bigint               as views,
    nullif(likes,    '')::bigint               as likes,
    nullif(comments, '')::bigint               as comments,
    duration                                    as duration_iso,
    category_id,
    thumbnail_url,
    nullif(fetched_date, '')::date             as fetched_date,
    nullif(ingestion_timestamp, '')::timestamp as ingestion_timestamp,
    status
from bronze
where status = 'clean'
