-- Total packets across temporal buckets must strictly equal staging total packets
with stg as (
    select count(*) as stg_total from {{ ref('stg_dns_packets') }}
),
mart as (
    select coalesce(sum(total_packets), 0) as mart_total from {{ ref('fct_temporal_metrics') }}
)
select *
from stg, mart
where stg_total != mart_total or stg_total = 0
