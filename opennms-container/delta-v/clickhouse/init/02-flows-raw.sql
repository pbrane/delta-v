CREATE TABLE IF NOT EXISTS deltav.flows_raw
(
    -- Timing and identity
    timestamp              DateTime64(3, 'UTC'),
    netflow_version        LowCardinality(String),
    direction              LowCardinality(String),
    sampling_algorithm     LowCardinality(String),
    sampling_interval      Nullable(Float64),
    clock_correction       UInt64,

    -- Volume and flow lifetime
    num_bytes              Nullable(UInt64),
    num_packets            Nullable(UInt64),
    num_flow_records       Nullable(UInt32),
    first_switched         Nullable(UInt64),
    last_switched          Nullable(UInt64),
    delta_switched         Nullable(UInt64),
    flow_seq_num           Nullable(UInt64),

    -- Source L3/L4
    src_address            IPv6,
    src_hostname           String,
    src_port               Nullable(UInt16),
    src_as                 Nullable(UInt64),
    src_mask_len           Nullable(UInt8),

    -- Destination L3/L4
    dst_address            IPv6,
    dst_hostname           String,
    dst_port               Nullable(UInt16),
    dst_as                 Nullable(UInt64),
    dst_mask_len           Nullable(UInt8),

    -- Next-hop (often absent in sFlow)
    next_hop_address       Nullable(IPv6),
    next_hop_hostname      String,

    -- Protocol / QoS / TCP
    protocol               Nullable(UInt8),
    ip_protocol_version    Nullable(UInt8),
    tcp_flags              Nullable(UInt32),
    tos                    Nullable(UInt8),
    dscp                   Nullable(UInt8),
    ecn                    Nullable(UInt8),
    vlan                   LowCardinality(String),

    -- Locality
    src_locality           LowCardinality(String),
    dst_locality           LowCardinality(String),
    flow_locality          LowCardinality(String),

    -- Classification
    application            LowCardinality(String),

    -- Exporter display metadata
    host                   String,
    location               LowCardinality(String),
    engine_id              Nullable(UInt32),
    engine_type            Nullable(UInt32),

    -- Exporter node inventory (from NodeInfo exporter_node)
    exporter_node_id               UInt32,
    exporter_node_foreign_source   LowCardinality(String),
    exporter_node_foreign_id       String,
    exporter_node_categories       Array(LowCardinality(String)),
    exporter_node_label            LowCardinality(String),
    input_snmp_ifindex             UInt32,
    output_snmp_ifindex            Nullable(UInt32),
    input_if_name                  LowCardinality(String),
    output_if_name                 LowCardinality(String),

    -- Source node inventory (from NodeInfo src_node)
    src_node_id                    UInt32,
    src_node_foreign_source        LowCardinality(String),
    src_node_foreign_id            String,
    src_node_categories            Array(LowCardinality(String)),

    -- Destination node inventory (from NodeInfo dest_node)
    dest_node_id                   UInt32,
    dest_node_foreign_source       LowCardinality(String),
    dest_node_foreign_id           String,
    dest_node_categories           Array(LowCardinality(String))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (exporter_node_id, timestamp, input_snmp_ifindex)
TTL timestamp + INTERVAL ${DELTAV_CLICKHOUSE_FLOWS_RAW_TTL_DAYS} DAY DELETE
SETTINGS index_granularity = 8192;

-- Additive migration for clusters created before these columns existed.
-- ADD COLUMN IF NOT EXISTS is a no-op once applied. Positions are chosen to
-- match the flows_ingest SELECT order (the materialized view inserts by
-- column position, so flows_raw column order must equal the SELECT order).
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS exporter_node_label LowCardinality(String) AFTER exporter_node_categories;
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS input_if_name  LowCardinality(String) AFTER output_snmp_ifindex;
ALTER TABLE deltav.flows_raw ADD COLUMN IF NOT EXISTS output_if_name LowCardinality(String) AFTER input_if_name;
