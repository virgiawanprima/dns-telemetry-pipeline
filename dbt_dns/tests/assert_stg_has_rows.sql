-- Prevents vacuous passes by asserting that staging is populated
select 1
where (select count(*) from {{ ref('stg_dns_packets') }}) = 0
