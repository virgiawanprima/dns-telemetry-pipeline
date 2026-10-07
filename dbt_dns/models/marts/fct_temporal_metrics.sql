{{ config(materialized='table') }}

with packets as (
    select * from {{ ref('stg_dns_packets') }}
)

select
    packet_minute,
    count(*) as total_packets,
    count(case when message_type = 0 then 1 end) as total_queries,
    count(case when message_type = 1 then 1 end) as total_responses,
    count(case when response_code = 3 then 1 end) as nxdomain_count,
    round(
        count(case when response_code = 3 then 1 end) * 100.0 /
        nullif(count(case when message_type = 1 then 1 end), 0),
        2
    ) as nxdomain_rate_pct,
    count(case when protocol = 'udp' and message_type = 1 then 1 end) as udp_responses,
    count(case when protocol = 'udp' and message_type = 1 and is_truncated = 1 then 1 end) as udp_truncated_count,
    round(
        count(case when protocol = 'udp' and message_type = 1 and is_truncated = 1 then 1 end) * 100.0 /
        nullif(count(case when protocol = 'udp' and message_type = 1 then 1 end), 0),
        2
    ) as udp_truncation_rate_pct
from packets
group by 1
order by 1
