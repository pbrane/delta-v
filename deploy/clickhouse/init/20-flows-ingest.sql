-- Recreated unconditionally (dropped in 03-flows-kafka.sql) so the SELECT stays
-- in sync with the flows_kafka schema and the flows_raw column order. The MV
-- inserts into flows_raw BY POSITION, so the projection order below must equal
-- the flows_raw column order exactly.
CREATE MATERIALIZED VIEW deltav.flows_ingest
TO deltav.flows_raw AS
SELECT
    toDateTime64(timestamp / 1000.0, 3, 'UTC')      AS timestamp,
    netflow_version,
    direction,
    sampling_algorithm,
    toNullable(sampling_interval.value)              AS sampling_interval,
    clock_correction,

    toNullable(num_bytes.value)                      AS num_bytes,
    toNullable(num_packets.value)                    AS num_packets,
    toNullable(num_flow_records.value)               AS num_flow_records,
    toNullable(first_switched.value)                 AS first_switched,
    toNullable(last_switched.value)                  AS last_switched,
    toNullable(delta_switched.value)                 AS delta_switched,
    toNullable(flow_seq_num.value)                   AS flow_seq_num,

    toIPv6(src_address)                              AS src_address,
    src_hostname,
    CAST(src_port.value AS Nullable(UInt16))          AS src_port,
    toNullable(src_as.value)                         AS src_as,
    CAST(src_mask_len.value AS Nullable(UInt8))       AS src_mask_len,

    toIPv6(dst_address)                              AS dst_address,
    dst_hostname,
    CAST(dst_port.value AS Nullable(UInt16))          AS dst_port,
    toNullable(dst_as.value)                         AS dst_as,
    CAST(dst_mask_len.value AS Nullable(UInt8))       AS dst_mask_len,

    toIPv6OrNull(next_hop_address)                   AS next_hop_address,
    next_hop_hostname,

    CAST(protocol.value AS Nullable(UInt8))           AS protocol,
    CAST(ip_protocol_version.value AS Nullable(UInt8)) AS ip_protocol_version,
    toNullable(tcp_flags.value)                      AS tcp_flags,
    CAST(tos.value AS Nullable(UInt8))                AS tos,
    CAST(dscp.value AS Nullable(UInt8))               AS dscp,
    CAST(ecn.value AS Nullable(UInt8))                AS ecn,
    vlan,

    src_locality,
    dst_locality,
    flow_locality,

    application,
    host,
    location,
    toNullable(engine_id.value)                      AS engine_id,
    toNullable(engine_type.value)                    AS engine_type,

    ifNull(exporter_node.node_id, 0)                 AS exporter_node_id,
    exporter_node.foreign_source                     AS exporter_node_foreign_source,
    exporter_node.foreign_id                         AS exporter_node_foreign_id,
    exporter_node.categories                         AS exporter_node_categories,
    exporter_node.node_label                         AS exporter_node_label,
    ifNull(input_snmp_ifindex.value, 0)              AS input_snmp_ifindex,
    toNullable(output_snmp_ifindex.value)            AS output_snmp_ifindex,
    input_if_name                                    AS input_if_name,
    output_if_name                                   AS output_if_name,

    ifNull(src_node.node_id, 0)                      AS src_node_id,
    src_node.foreign_source                          AS src_node_foreign_source,
    src_node.foreign_id                              AS src_node_foreign_id,
    src_node.categories                              AS src_node_categories,

    ifNull(dest_node.node_id, 0)                     AS dest_node_id,
    dest_node.foreign_source                         AS dest_node_foreign_source,
    dest_node.foreign_id                             AS dest_node_foreign_id,
    dest_node.categories                             AS dest_node_categories
FROM deltav.flows_kafka;
