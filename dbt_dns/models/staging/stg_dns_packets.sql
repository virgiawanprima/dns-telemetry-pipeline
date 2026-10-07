{{ config(materialized='view') }}

with source_data as (
    select *
    from read_parquet('data/sample-dns-30min.parquet')
)

select
    to_timestamp(ts) as packet_timestamp,
    date_trunc('minute', to_timestamp(ts)) as packet_minute,
    cast(ip_ver as integer) as ip_version,
    lower(cast(proto as varchar)) as protocol,
    src_ip,
    cast(src_port as integer) as src_port,
    dst_ip,
    cast(dst_port as integer) as dst_port,
    cast(frame_len as integer) as frame_length_bytes,
    cast(dns_len as integer) as dns_length_bytes,
    cast(dns_id as integer) as transaction_id,
    cast(qr as integer) as message_type, -- 0=Query, 1=Response
    case when qr = 0 then 'QUERY' else 'RESPONSE' end as message_type_name,
    cast(opcode as integer) as opcode,
    cast(aa as integer) as is_authoritative,
    cast(tc as integer) as is_truncated,
    cast(rd as integer) as is_recursion_desired,
    cast(ra as integer) as is_recursion_available,
    cast(rcode as integer) as response_code,
    cast(qdcount as integer) as question_count,
    cast(ancount as integer) as answer_count,
    qname,
    cast(qtype as integer) as qtype,
    cast(qtype_name as varchar) as qtype_name,
    cast(edns as integer) as is_edns_supported,
    cast(edns_udpsize as integer) as edns_buffer_size,
    case
        when ip_ver = 6 and src_ip is not null then
            regexp_extract(src_ip, '^([0-9a-fA-F:]+:[0-9a-fA-F:]+:[0-9a-fA-F:]+)', 1)
        when ip_ver = 4 and src_ip is not null then
            regexp_extract(src_ip, '^([0-9]+\.[0-9]+\.[0-9]+)', 1)
        else 'UNKNOWN'
    end as src_prefix_48
from source_data
