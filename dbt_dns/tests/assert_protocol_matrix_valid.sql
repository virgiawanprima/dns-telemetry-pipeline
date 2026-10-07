-- Percentage distribution of responses per QTYPE must sum to approximately 100%
select
    qtype_name,
    round(sum(row_percentage), 1) as total_share
from {{ ref('fct_protocol_matrix') }}
group by qtype_name
having round(sum(row_percentage), 1) not between 99.0 and 101.0
