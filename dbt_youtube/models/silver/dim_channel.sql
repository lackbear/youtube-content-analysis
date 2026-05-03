{{ config(materialized='table', tags=['silver']) }}

-- Channel dimension table — one row per channel ever tracked.
--
-- Sourced from competitors.csv (chapter 6 commit 1). All-varchar at the
-- bronze read level; this model casts each column to its real type and
-- normalises the active flag to a proper boolean. Inactive channels are
-- kept (NOT filtered out) so historical bronze rows that reference an
-- old channel_id still resolve a name + niche when joined to gold.
--
-- Joinable to:
--   stg_video_stats USING (channel_id)
--   fct_video_growth_7d USING (channel_id)
--
-- Future fields (chapter 6 commits 2/3): last_active_date, posts_per_week,
-- discovered_via (which discover.py run suggested this channel).

with src as (
    select * from {{ source('bronze', 'competitors') }}
)

select
    handle,
    channel_id,
    name,
    niche,
    tier,
    nullif(subscribers_at_addition, '')::bigint as subscribers_at_addition,
    nullif(added_date, '')::date                as added_date,
    lower(active) = 'true'                       as active,
    nullif(deactivated_date, '')::date          as deactivated_date,
    nullif(deactivated_reason, '')              as deactivated_reason
from src
