{{ config(materialized='table', tags=['gold']) }}

-- 7-day growth per video.
--
-- Grain: one row per (video_id, snapshot_date).
--
-- Method: ASOF LEFT JOIN — for each snapshot, find the most-recent prior
-- snapshot of the same video at or before (snapshot_date - 7 days). This
-- is robust to gaps in collection cadence: if there's no snapshot exactly
-- 7 days ago, ASOF picks the nearest earlier one and `days_since_baseline`
-- records the actual lag, so consumers know how loose the window was.
--
-- Rows whose video has no qualifying baseline (first ~week of life of
-- a video) are dropped — a 7-day delta with no baseline isn't a delta.

with snapshots as (
    select * from {{ ref('stg_video_stats') }}
)

select
    s.video_id,
    s.channel_id,
    s.channel_name,
    s.title,
    s.fetched_date                                          as snapshot_date,
    p.fetched_date                                          as baseline_date,
    cast(s.fetched_date - p.fetched_date as integer)        as days_since_baseline,
    s.views,
    s.likes,
    s.comments,
    p.views                                                 as views_baseline,
    p.likes                                                 as likes_baseline,
    p.comments                                              as comments_baseline,
    s.views    - p.views                                    as views_added_window,
    s.likes    - p.likes                                    as likes_added_window,
    s.comments - p.comments                                 as comments_added_window
from snapshots s
asof left join snapshots p
    on  s.video_id = p.video_id
    and p.fetched_date <= s.fetched_date - interval '7' day
where p.fetched_date is not null
