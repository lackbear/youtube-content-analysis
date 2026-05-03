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

select
    video_id,
    channel_id,
    channel_name,
    title,
    cast(published_at as timestamp)        as published_at,
    cast(views    as bigint)               as views,
    cast(likes    as bigint)               as likes,
    cast(comments as bigint)               as comments,
    duration                                as duration_iso,
    category_id,
    thumbnail_url,
    cast(fetched_date as date)             as fetched_date,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
    status
from bronze
where status = 'clean'
