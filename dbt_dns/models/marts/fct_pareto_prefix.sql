{{ config(materialized='table') }}

with queries as (
    select * from {{ ref('stg_dns_packets') }}
    where message_type = 0 and src_prefix_48 is not null
),

total_volume as (
    select count(*) as global_query_count from queries
),

prefix_counts as (
    select
        src_prefix_48,
        count(*) as query_count
    from queries
    group by 1
),

ranked as (
    select
        p.src_prefix_48,
        p.query_count,
        round(p.query_count * 100.0 / t.global_query_count, 3) as share_pct,
        round(
            sum(p.query_count) over (order by p.query_count desc rows between unbounded preceding and current row) * 100.0 / t.global_query_count,
            3
        ) as cumulative_share_pct,
        row_number() over (order by p.query_count desc) as prefix_rank
    from prefix_counts p
    cross join total_volume t
)

select
    prefix_rank,
    src_prefix_48,
    query_count,
    share_pct,
    cumulative_share_pct,
    case when cumulative_share_pct <= 80.0 then true else false end as is_pareto_top_80
from ranked
order by prefix_rank
