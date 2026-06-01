-- flows_ingest reads flows_kafka; drop it before recreating the kafka table so
-- no materialized view is left attached to a dropped table. It is recreated in
-- 20-flows-ingest.sql. Both objects are stateless (a Kafka consumer view and a
-- streaming MV), so DROP+recreate is the clean migration for schema changes;
-- the consumer rejoins group 'deltav-clickhouse-persister' at its committed
-- offset, so no messages are lost.
DROP VIEW IF EXISTS deltav.flows_ingest;
DROP TABLE IF EXISTS deltav.flows_kafka;

CREATE TABLE deltav.flows_kafka
(
    -- Timing and identity
    timestamp               UInt64,
    netflow_version         String,
    direction               String,
    sampling_algorithm      String,
    sampling_interval       Tuple(value Float64),
    clock_correction        UInt64,

    -- Volume and flow lifetime
    num_bytes               Tuple(value UInt64),
    num_packets             Tuple(value UInt64),
    num_flow_records        Tuple(value UInt32),
    first_switched          Tuple(value UInt64),
    last_switched           Tuple(value UInt64),
    delta_switched          Tuple(value UInt64),
    flow_seq_num            Tuple(value UInt64),

    -- Source L3/L4
    src_address             String,
    src_hostname            String,
    src_port                Tuple(value UInt32),
    src_as                  Tuple(value UInt64),
    src_mask_len            Tuple(value UInt32),

    -- Destination L3/L4
    dst_address             String,
    dst_hostname            String,
    dst_port                Tuple(value UInt32),
    dst_as                  Tuple(value UInt64),
    dst_mask_len            Tuple(value UInt32),

    -- Next-hop
    next_hop_address        String,
    next_hop_hostname       String,

    -- Protocol / QoS / TCP
    protocol                Tuple(value UInt32),
    ip_protocol_version     Tuple(value UInt32),
    tcp_flags               Tuple(value UInt32),
    tos                     Tuple(value UInt32),
    dscp                    Tuple(value UInt32),
    ecn                     Tuple(value UInt32),
    vlan                    String,

    -- Locality
    src_locality            String,
    dst_locality            String,
    flow_locality           String,

    -- Classification
    application             String,

    -- Exporter display metadata
    host                    String,
    location                String,
    engine_id               Tuple(value UInt32),
    engine_type             Tuple(value UInt32),

    -- Exporter/src/dest NodeInfo (as Tuple, name-matched to proto)
    src_node                Tuple(
        node_id             UInt32,
        foreign_source      String,
        foreign_id          String,
        categories          Array(String)
    ),
    exporter_node           Tuple(
        node_id             UInt32,
        foreign_source      String,
        foreign_id          String,
        categories          Array(String),
        node_label          String
    ),
    dest_node               Tuple(
        node_id             UInt32,
        foreign_source      String,
        foreign_id          String,
        categories          Array(String)
    ),

    -- SNMP ifindex lives at the top level of the proto, not in NodeInfo
    input_snmp_ifindex      Tuple(value UInt32),
    output_snmp_ifindex     Tuple(value UInt32),

    -- Resolved interface names (top-level proto strings)
    input_if_name           String,
    output_if_name          String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:9092',
    kafka_topic_list           = 'deltav-flows',
    kafka_group_name           = 'deltav-clickhouse-persister',
    kafka_format               = 'ProtobufSingle',
    kafka_schema               = 'deltav-flows.proto:FlowDocument',
    kafka_num_consumers        = 2,
    kafka_max_block_size       = 65536,
    kafka_skip_broken_messages = 100;
