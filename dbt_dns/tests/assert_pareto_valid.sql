-- Cumulative share of rank 1 prefix must be strictly greater than 0 and at most 100
select *
from {{ ref('fct_pareto_prefix') }}
where prefix_rank = 1 and (cumulative_share_pct <= 0.0 or cumulative_share_pct > 100.0)
