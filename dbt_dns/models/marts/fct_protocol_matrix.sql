{{ config(materialized='table') }}

with responses as (
    select * from {{ ref('stg_dns_packets') }}
    where message_type = 1 and qtype_name is not null
),

matrix as (
    select
        qtype_name,
        response_code,
        count(*) as response_count
    from responses
    group by 1, 2
),

totals_per_qtype as (
    select
        qtype_name,
        sum(response_count) as total_qtype_responses
    from matrix
    group by 1
)

select
    m.qtype_name,
    m.response_code,
    m.response_count,
    round(m.response_count * 100.0 / t.total_qtype_responses, 2) as row_percentage
from matrix m
join totals_per_qtype t on m.qtype_name = t.qtype_name
where t.total_qtype_responses >= 1000
order by t.total_qtype_responses desc, m.response_count desc
