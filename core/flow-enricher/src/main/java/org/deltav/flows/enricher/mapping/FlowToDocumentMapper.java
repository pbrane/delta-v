/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.flows.enricher.mapping;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;

import org.deltav.flows.proto.FlowDocumentProtos;
import org.opennms.integration.api.v1.flows.Flow.Direction;
import org.opennms.integration.api.v1.flows.Flow.NetflowVersion;
import org.opennms.integration.api.v1.flows.Flow.SamplingAlgorithm;
import org.opennms.netmgt.flows.api.Flow;

/**
 * Converts a horizon {@link Flow} plus enriched {@link NodeIdentity}
 * records, an application classification string, and locality enrichment
 * strings into a {@link FlowDocumentProtos.FlowDocument} message ready for
 * publication to the {@code deltav-flows} Kafka topic.
 *
 * <p><strong>Null-wrapper discipline.</strong> Horizon's {@link Flow}
 * interface exposes most numeric fields as boxed wrapper types (for example,
 * {@code Long getBytes()}) that may return {@code null} when the upstream
 * exporter omitted the field from the flow record. The delta-v protobuf
 * schema mirrors this with {@code google.protobuf.UInt64Value} and similar
 * wrapper types, which are distinct messages whose presence can be queried
 * with {@code hasXxx()}. This mapper carefully populates each wrapper-typed
 * field <em>only</em> when the horizon getter returned a non-null value, so
 * downstream consumers (ClickHouse column schemas, queries over the
 * {@code deltav-flows} topic) can cleanly distinguish "the exporter did not
 * report this value" from "the exporter reported zero."
 *
 * <p>Stateless and thread-safe.
 */
public class FlowToDocumentMapper {

    /**
     * Data-source-agnostic carrier for node identity fields resolved from
     * either the NodeContext in-memory cache or any other source.
     *
     * @param nodeId        OpenNMS node ID
     * @param foreignSource requisition foreign source, may be {@code null}
     * @param foreignId     requisition foreign ID, may be {@code null}
     * @param nodeLabel     human-readable node label, may be empty or {@code null}
     * @param categories    OpenNMS node categories; empty list when unknown
     */
    public record NodeIdentity(
            int nodeId,
            String foreignSource,
            String foreignId,
            String nodeLabel,
            List<String> categories) {}

    /**
     * Build a {@link FlowDocumentProtos.FlowDocument} from a horizon {@link Flow}
     * and its enrichment context.
     *
     * @param flow             the parsed horizon flow record
     * @param exporterNodeInfo resolved node identity for the exporter, or {@code null}
     *                         when no matching node was found
     * @param srcNodeInfo      resolved node identity for the source IP, or {@code null}
     * @param destNodeInfo     resolved node identity for the destination IP, or {@code null}
     * @param application      application classification string (e.g. {@code "HTTPS"}),
     *                         or {@code null} which maps to {@code "unknown"}
     * @param srcLocality      source locality (one of {@code "PRIVATE"} / {@code "PUBLIC"},
     *                         or {@code null} / other values for unknown)
     * @param dstLocality      destination locality (same semantics as {@code srcLocality})
     * @param flowLocality     aggregate flow locality (same semantics as {@code srcLocality})
     * @param host             hostname or FQDN of the exporter (typically derived from
     *                         {@code FlowSource.getSourceAddress()}). Empty or {@code null}
     *                         leaves the proto field unset (empty string default).
     * @param location         Minion location string (typically derived from
     *                         {@code FlowSource.getLocation()}). Empty or {@code null}
     *                         leaves the proto field unset.
     * @param clockCorrection  clock-skew correction in milliseconds applied to timestamps.
     *                         Phase 1.5 always passes {@code 0}; Phase 1.6 will compute
     *                         actual skew. Always written to the proto (0 is a valid value).
     * @return a fully populated {@code FlowDocument} proto message
     */
    public FlowDocumentProtos.FlowDocument map(
            Flow flow,
            NodeIdentity exporterNodeInfo,
            NodeIdentity srcNodeInfo,
            NodeIdentity destNodeInfo,
            String application,
            String srcLocality,
            String dstLocality,
            String flowLocality,
            String host,
            String location,
            long clockCorrection,
            String inputIfName,
            String outputIfName) {

        FlowDocumentProtos.FlowDocument.Builder builder = FlowDocumentProtos.FlowDocument.newBuilder();

        // Top-level timestamp (proto uint64 timestamp, always set from Instant).
        final Instant timestamp = flow.getTimestamp();
        if (timestamp != null) {
            builder.setTimestamp(timestamp.toEpochMilli());
        }

        // UInt64Value wrapper-typed fields: set only when the flow getter is non-null.
        final Long bytes = flow.getBytes();
        if (bytes != null) {
            builder.setNumBytes(UInt64Value.of(bytes));
        }
        final Long packets = flow.getPackets();
        if (packets != null) {
            builder.setNumPackets(UInt64Value.of(packets));
        }
        final Long dstAs = flow.getDstAs();
        if (dstAs != null) {
            builder.setDstAs(UInt64Value.of(dstAs));
        }
        final Long srcAs = flow.getSrcAs();
        if (srcAs != null) {
            builder.setSrcAs(UInt64Value.of(srcAs));
        }
        final Instant firstSwitched = flow.getFirstSwitched();
        if (firstSwitched != null) {
            builder.setFirstSwitched(UInt64Value.of(firstSwitched.toEpochMilli()));
        }
        final Instant lastSwitched = flow.getLastSwitched();
        if (lastSwitched != null) {
            builder.setLastSwitched(UInt64Value.of(lastSwitched.toEpochMilli()));
        }
        final Instant deltaSwitched = flow.getDeltaSwitched();
        if (deltaSwitched != null) {
            builder.setDeltaSwitched(UInt64Value.of(deltaSwitched.toEpochMilli()));
        }

        // Flow sequence number is a primitive long in horizon — always set.
        builder.setFlowSeqNum(UInt64Value.of(flow.getFlowSeqNum()));

        // Flow record count is a primitive int in horizon — always set.
        builder.setNumFlowRecords(UInt32Value.of(flow.getFlowRecords()));

        // UInt32Value wrapper-typed fields: set only when non-null.
        final Integer dstMaskLen = flow.getDstMaskLen();
        if (dstMaskLen != null) {
            builder.setDstMaskLen(UInt32Value.of(dstMaskLen));
        }
        final Integer dstPort = flow.getDstPort();
        if (dstPort != null) {
            builder.setDstPort(UInt32Value.of(dstPort));
        }
        final Integer srcMaskLen = flow.getSrcMaskLen();
        if (srcMaskLen != null) {
            builder.setSrcMaskLen(UInt32Value.of(srcMaskLen));
        }
        final Integer srcPort = flow.getSrcPort();
        if (srcPort != null) {
            builder.setSrcPort(UInt32Value.of(srcPort));
        }
        final Integer engineId = flow.getEngineId();
        if (engineId != null) {
            builder.setEngineId(UInt32Value.of(engineId));
        }
        final Integer engineType = flow.getEngineType();
        if (engineType != null) {
            builder.setEngineType(UInt32Value.of(engineType));
        }
        final Integer inputSnmp = flow.getInputSnmp();
        if (inputSnmp != null) {
            builder.setInputSnmpIfindex(UInt32Value.of(inputSnmp));
        }
        final Integer outputSnmp = flow.getOutputSnmp();
        if (outputSnmp != null) {
            builder.setOutputSnmpIfindex(UInt32Value.of(outputSnmp));
        }
        final Integer ipProtoVersion = flow.getIpProtocolVersion();
        if (ipProtoVersion != null) {
            builder.setIpProtocolVersion(UInt32Value.of(ipProtoVersion));
        }
        final Integer protocol = flow.getProtocol();
        if (protocol != null) {
            builder.setProtocol(UInt32Value.of(protocol));
        }
        final Integer tcpFlags = flow.getTcpFlags();
        if (tcpFlags != null) {
            builder.setTcpFlags(UInt32Value.of(tcpFlags));
        }
        final Integer tos = flow.getTos();
        if (tos != null) {
            builder.setTos(UInt32Value.of(tos));
        }
        final Integer dscp = flow.getDscp();
        if (dscp != null) {
            builder.setDscp(UInt32Value.of(dscp));
        }
        final Integer ecn = flow.getEcn();
        if (ecn != null) {
            builder.setEcn(UInt32Value.of(ecn));
        }

        // DoubleValue wrapper-typed fields: set only when non-null.
        final Double samplingInterval = flow.getSamplingInterval();
        if (samplingInterval != null) {
            builder.setSamplingInterval(DoubleValue.of(samplingInterval));
        }

        // Plain string fields — proto3 scalar strings default to "" and cannot
        // distinguish "unset" from "empty", but we still avoid setting null.
        final String srcAddr = flow.getSrcAddr();
        if (srcAddr != null) {
            builder.setSrcAddress(srcAddr);
        }
        final String dstAddr = flow.getDstAddr();
        if (dstAddr != null) {
            builder.setDstAddress(dstAddr);
        }
        final String nextHop = flow.getNextHop();
        if (nextHop != null) {
            builder.setNextHopAddress(nextHop);
        }

        // Optional<String> hostname getters.
        final Optional<String> srcHostname = flow.getSrcAddrHostname();
        srcHostname.ifPresent(builder::setSrcHostname);
        final Optional<String> dstHostname = flow.getDstAddrHostname();
        dstHostname.ifPresent(builder::setDstHostname);
        final Optional<String> nextHopHostname = flow.getNextHopHostname();
        nextHopHostname.ifPresent(builder::setNextHopHostname);

        // VLAN: horizon returns Integer, proto field is a string (historical schema).
        final Integer vlan = flow.getVlan();
        if (vlan != null) {
            builder.setVlan(Integer.toString(vlan));
        }

        // Enums.
        builder.setDirection(mapDirection(flow.getDirection()));
        builder.setNetflowVersion(mapNetflowVersion(flow.getNetflowVersion()));
        builder.setSamplingAlgorithm(mapSamplingAlgorithm(flow.getSamplingAlgorithm()));

        // NodeInfo sub-messages: set only when enrichment provided a non-null lookup.
        if (exporterNodeInfo != null) {
            builder.setExporterNode(toProtoNodeInfo(exporterNodeInfo));
        }
        if (srcNodeInfo != null) {
            builder.setSrcNode(toProtoNodeInfo(srcNodeInfo));
        }
        if (destNodeInfo != null) {
            builder.setDestNode(toProtoNodeInfo(destNodeInfo));
        }

        // Application classification: default to "unknown" when null.
        builder.setApplication(application != null ? application : "unknown");

        // Locality enrichment.
        builder.setSrcLocality(mapLocality(srcLocality));
        builder.setDstLocality(mapLocality(dstLocality));
        builder.setFlowLocality(mapLocality(flowLocality));

        // Exporter host/location enrichment (derived from FlowSource by the caller).
        // Both are proto3 scalar strings that default to "" when unset, so we only
        // write non-null, non-empty values to keep "unknown" distinguishable from
        // "reported but empty".
        if (host != null && !host.isEmpty()) {
            builder.setHost(host);
        }
        if (location != null && !location.isEmpty()) {
            builder.setLocation(location);
        }

        // Exporter SNMP interface names (resolved by the caller from the
        // exporter node's snmpinterface). proto3 scalar strings default to "",
        // so only write non-empty values.
        if (inputIfName != null && !inputIfName.isEmpty()) {
            builder.setInputIfName(inputIfName);
        }
        if (outputIfName != null && !outputIfName.isEmpty()) {
            builder.setOutputIfName(outputIfName);
        }

        // Clock skew correction (milliseconds). Plain uint64; 0 is a valid value
        // meaning "no correction applied", so we always set it.
        builder.setClockCorrection(clockCorrection);

        return builder.build();
    }

    /**
     * Overload for callers without resolved exporter interface names; delegates
     * with {@code null} {@code inputIfName}/{@code outputIfName}.
     */
    public FlowDocumentProtos.FlowDocument map(
            Flow flow,
            NodeIdentity exporterNodeInfo,
            NodeIdentity srcNodeInfo,
            NodeIdentity destNodeInfo,
            String application,
            String srcLocality,
            String dstLocality,
            String flowLocality,
            String host,
            String location,
            long clockCorrection) {
        return map(flow, exporterNodeInfo, srcNodeInfo, destNodeInfo, application,
                srcLocality, dstLocality, flowLocality, host, location, clockCorrection,
                null, null);
    }

    private static FlowDocumentProtos.NodeInfo toProtoNodeInfo(NodeIdentity info) {
        FlowDocumentProtos.NodeInfo.Builder b = FlowDocumentProtos.NodeInfo.newBuilder();
        b.setNodeId(info.nodeId());
        if (info.foreignSource() != null) {
            b.setForeignSource(info.foreignSource());
        }
        if (info.foreignId() != null) {
            b.setForeignId(info.foreignId());
        }
        if (info.nodeLabel() != null && !info.nodeLabel().isEmpty()) {
            b.setNodeLabel(info.nodeLabel());
        }
        if (info.categories() != null) {
            b.addAllCategories(info.categories());
        }
        return b.build();
    }

    private static FlowDocumentProtos.Direction mapDirection(Direction direction) {
        if (direction == null) {
            return FlowDocumentProtos.Direction.DIRECTION_UNKNOWN;
        }
        return switch (direction) {
            case INGRESS -> FlowDocumentProtos.Direction.INGRESS;
            case EGRESS -> FlowDocumentProtos.Direction.EGRESS;
            case UNKNOWN -> FlowDocumentProtos.Direction.DIRECTION_UNKNOWN;
        };
    }

    private static FlowDocumentProtos.NetflowVersion mapNetflowVersion(NetflowVersion version) {
        if (version == null) {
            return FlowDocumentProtos.NetflowVersion.NETFLOW_VERSION_UNKNOWN;
        }
        return switch (version) {
            case V5 -> FlowDocumentProtos.NetflowVersion.V5;
            case V9 -> FlowDocumentProtos.NetflowVersion.V9;
            case IPFIX -> FlowDocumentProtos.NetflowVersion.IPFIX;
            case SFLOW -> FlowDocumentProtos.NetflowVersion.SFLOW;
        };
    }

    private static FlowDocumentProtos.SamplingAlgorithm mapSamplingAlgorithm(SamplingAlgorithm algorithm) {
        if (algorithm == null) {
            return FlowDocumentProtos.SamplingAlgorithm.SAMPLING_ALGORITHM_UNKNOWN;
        }
        return switch (algorithm) {
            case Unassigned -> FlowDocumentProtos.SamplingAlgorithm.SAMPLING_ALGORITHM_UNKNOWN;
            case SystematicCountBasedSampling ->
                    FlowDocumentProtos.SamplingAlgorithm.SYSTEMATIC_COUNT_BASED_SAMPLING;
            case SystematicTimeBasedSampling ->
                    FlowDocumentProtos.SamplingAlgorithm.SYSTEMATIC_TIME_BASED_SAMPLING;
            case RandomNOutOfNSampling ->
                    FlowDocumentProtos.SamplingAlgorithm.RANDOM_N_OUT_OF_N_SAMPLING;
            case UniformProbabilisticSampling ->
                    FlowDocumentProtos.SamplingAlgorithm.UNIFORM_PROBABILISTIC_SAMPLING;
            case PropertyMatchFiltering ->
                    FlowDocumentProtos.SamplingAlgorithm.PROPERTY_MATCH_FILTERING;
            case HashBasedFiltering ->
                    FlowDocumentProtos.SamplingAlgorithm.HASH_BASED_FILTERING;
            case FlowStateDependentIntermediateFlowSelectionProcess ->
                    FlowDocumentProtos.SamplingAlgorithm.FLOW_STATE_DEPENDENT_INTERMEDIATE_FLOW_SELECTION_PROCESS;
        };
    }

    private static FlowDocumentProtos.Locality mapLocality(String locality) {
        if (locality == null) {
            return FlowDocumentProtos.Locality.LOCALITY_UNKNOWN;
        }
        return switch (locality) {
            case "PRIVATE" -> FlowDocumentProtos.Locality.PRIVATE;
            case "PUBLIC" -> FlowDocumentProtos.Locality.PUBLIC;
            default -> FlowDocumentProtos.Locality.LOCALITY_UNKNOWN;
        };
    }
}
