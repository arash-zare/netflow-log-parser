-- Ensure database exists
CREATE DATABASE IF NOT EXISTS netflow;

-- Cleanup old objects
DROP VIEW IF EXISTS netflow.flows_mv;
DROP TABLE IF EXISTS netflow.flows_kafka;
DROP TABLE IF EXISTS netflow.flows;

-- Destination table (where clean data lands)
CREATE TABLE netflow.flows
(
    ts                    DateTime64(3, 'Asia/Tehran'),
    type                  String,
    time_received_ns      UInt64,
    sequence_num          UInt64,
    sampling_rate         UInt32,
    sampler_address       String,
    time_flow_start_ns    UInt64,
    time_flow_end_ns      UInt64,
    bytes                 UInt64,
    packets               UInt64,
    src_addr              String,
    dst_addr              String,
    etype                 String,
    proto                 String,
    src_port              UInt16,
    dst_port              UInt16,
    in_if                 UInt32,
    out_if                UInt32,
    src_mac               String,
    dst_mac               String,
    src_vlan              UInt16,
    dst_vlan              UInt16,
    vlan_id               UInt16,
    ip_tos                UInt8,
    forwarding_status     UInt8,
    ip_ttl                UInt8,
    ip_flags              UInt16,
    tcp_flags             UInt8,
    icmp_type             UInt8,
    icmp_code             UInt8,
    ipv6_flow_label       UInt32,
    fragment_id           UInt32,
    fragment_offset       UInt16,
    src_as                UInt32,
    dst_as                UInt32,
    next_hop              String,
    next_hop_as           UInt32,
    src_net               String,
    dst_net               String,
    bgp_next_hop          String,
    bgp_communities       Array(String),
    as_path               Array(String),
    mpls_ttl              Array(String),
    mpls_label            Array(String),
    mpls_ip               Array(String),
    observation_domain_id UInt32,
    observation_point_id  UInt32,
    layer_stack           Array(String),
    layer_size            Array(String),
    ipv6_routing_header_addresses Array(String),
    ipv6_routing_header_seg_left UInt8
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (ts, src_addr, dst_addr)
TTL ts + INTERVAL 1 HOUR
SETTINGS index_granularity = 8192;


-- Kafka source table
CREATE TABLE netflow.flows_kafka
(
    type String,
    time_received_ns UInt64,
    sequence_num UInt64,
    sampling_rate UInt32,
    sampler_address String,
    time_flow_start_ns UInt64,
    time_flow_end_ns UInt64,
    bytes UInt64,
    packets UInt64,
    src_addr String,
    dst_addr String,
    etype String,
    proto String,
    src_port UInt16,
    dst_port UInt16,
    in_if UInt32,
    out_if UInt32,
    src_mac String,
    dst_mac String,
    src_vlan UInt16,
    dst_vlan UInt16,
    vlan_id UInt16,
    ip_tos UInt8,
    forwarding_status UInt8,
    ip_ttl UInt8,
    ip_flags UInt16,
    tcp_flags UInt8,
    icmp_type UInt8,
    icmp_code UInt8,
    ipv6_flow_label UInt32,
    fragment_id UInt32,
    fragment_offset UInt16,
    src_as UInt32,
    dst_as UInt32,
    next_hop String,
    next_hop_as UInt32,
    src_net String,
    dst_net String,
    bgp_next_hop String,
    bgp_communities Array(String),
    as_path Array(String),
    mpls_ttl Array(String),
    mpls_label Array(String),
    mpls_ip Array(String),
    observation_domain_id UInt32,
    observation_point_id UInt32,
    layer_stack Array(String),
    layer_size Array(String),
    ipv6_routing_header_addresses Array(String),
    ipv6_routing_header_seg_left UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:9092',
    kafka_topic_list           = 'flows',
    kafka_group_name           = 'ch-netflow-consumers',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 5,
    kafka_max_block_size       = 32768,
    kafka_skip_broken_messages = 1;

-- Materialized view (Kafka -> flows)
CREATE MATERIALIZED VIEW netflow.flows_mv
TO netflow.flows
AS
SELECT
    toDateTime64(divide(time_received_ns, 1000000000), 3, 'Asia/Tehran') AS ts,
    *
FROM netflow.flows_kafka;

