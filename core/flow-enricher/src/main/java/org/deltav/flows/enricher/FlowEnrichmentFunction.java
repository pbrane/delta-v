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
package org.deltav.flows.enricher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.deltav.flows.enricher.classification.ApplicationClassifier;
import org.deltav.flows.enricher.enrichment.FlowLocalityCalculator;
import org.deltav.flows.enricher.enrichment.FlowLocalityCalculator.Locality;
import org.deltav.flows.enricher.enrichment.InterfaceMarkingCache;
import org.deltav.flows.enricher.enrichment.JdbcNodeInfoLookup;
import org.deltav.flows.enricher.enrichment.JdbcSnmpInterfaceLookup;
import org.deltav.flows.enricher.mapping.FlowToDocumentMapper;
import org.deltav.flows.enricher.protocol.ProtocolMessageProcessor;
import org.deltav.flows.proto.FlowDocumentProtos;
import org.opennms.netmgt.flows.api.Flow;
import org.opennms.netmgt.telemetry.common.ipc.TelemetryProtos.TelemetryMessageLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

/**
 * Phase 1.5 (Commit 5) flow enrichment pipeline. Consumes a Spring Cloud
 * Stream {@code Message<byte[]>} carrying the raw Kafka Sink payload plus the
 * Kafka {@code kafka_receivedTopic} header, dispatches to a protocol-specific
 * {@link ProtocolMessageProcessor} by module ID, enriches each parsed
 * {@link Flow}, maps it to a {@link FlowDocumentProtos.FlowDocument}, and
 * returns the list of serialized document byte arrays ready to publish on
 * the outbound {@code deltav-flows} topic.
 *
 * <h2>Why {@code Function<Message<byte[]>, List<byte[]>>}?</h2>
 *
 * <p>The splitter-style return type ({@code List<byte[]>}) lets one input
 * message produce zero, one, or many output records; Spring Cloud Stream
 * interprets an empty list as "drop this input without producing any output."
 * The {@link Message}-typed input lets us read the Kafka topic name from the
 * {@link KafkaHeaders#RECEIVED_TOPIC} header. The Sink protobuf envelope does
 * <em>not</em> carry a module ID field, so the topic name is the only place
 * to recover it.
 *
 * <h2>Module ID extraction</h2>
 *
 * <p>The Kafka topic follows the scheme {@code DeltaV.Sink.<moduleId>}.
 * The prefix is stripped to yield a module ID such as
 * {@code Telemetry-Netflow-5}, which is used as the dispatch key into the
 * processor map. Any other prefix (including a missing header) causes the
 * function to drop the message.
 *
 * <h2>Per-flow enrichment</h2>
 *
 * <p>For each parsed flow, the pipeline:
 * <ol>
 *   <li>Looks up the exporter node info once from
 *       {@code messageLog.getSourceAddress()} (reused across all flows in
 *       this message log).</li>
 *   <li>Looks up src and dst node info from {@code flow.getSrcAddr()} and
 *       {@code flow.getDstAddr()}.</li>
 *   <li>Computes src, dst, and aggregate flow locality via
 *       {@link FlowLocalityCalculator}.</li>
 *   <li>Marks the exporter's input and output SNMP interfaces (only when
 *       the exporter node is known and the ifindex is set and positive).</li>
 *   <li>Classifies the flow's application from its
 *       dst-port/src-port/protocol via {@link ApplicationClassifier}.</li>
 *   <li>Maps the enriched flow to a
 *       {@link FlowDocumentProtos.FlowDocument} via
 *       {@link FlowToDocumentMapper}.</li>
 *   <li>Serializes the document to bytes and appends it to the result list.</li>
 * </ol>
 *
 * <p>If enriching or mapping one flow throws an unchecked exception, that
 * flow is skipped with a {@code WARN} log and the remaining flows in the
 * message log continue to be processed. A single malformed flow must not
 * poison the entire batch.
 *
 * <p>Phase 1.5 always passes {@code 0L} as the clock-correction argument to
 * the mapper; Phase 1.6 will compute actual exporter clock skew.
 */
public class FlowEnrichmentFunction {

    private static final Logger LOG = LoggerFactory.getLogger(FlowEnrichmentFunction.class);

    private static final String DELTAV_SINK_PREFIX = "DeltaV.Sink.";

    private final SinkMessageDeserializer deserializer;
    private final JdbcNodeInfoLookup nodeInfoLookup;
    private final FlowLocalityCalculator localityCalculator;
    private final InterfaceMarkingCache interfaceMarkingCache;
    private final ApplicationClassifier applicationClassifier;
    private final FlowToDocumentMapper flowToDocumentMapper;
    private final JdbcSnmpInterfaceLookup snmpInterfaceLookup;
    private final Map<String, ProtocolMessageProcessor> processorsByModuleId;

    public FlowEnrichmentFunction(
            SinkMessageDeserializer deserializer,
            JdbcNodeInfoLookup nodeInfoLookup,
            FlowLocalityCalculator localityCalculator,
            InterfaceMarkingCache interfaceMarkingCache,
            ApplicationClassifier applicationClassifier,
            FlowToDocumentMapper flowToDocumentMapper,
            JdbcSnmpInterfaceLookup snmpInterfaceLookup,
            Map<String, ProtocolMessageProcessor> processorsByModuleId) {
        this.deserializer = deserializer;
        this.nodeInfoLookup = nodeInfoLookup;
        this.localityCalculator = localityCalculator;
        this.interfaceMarkingCache = interfaceMarkingCache;
        this.applicationClassifier = applicationClassifier;
        this.flowToDocumentMapper = flowToDocumentMapper;
        this.snmpInterfaceLookup = snmpInterfaceLookup;
        this.processorsByModuleId = Map.copyOf(processorsByModuleId);
    }

    /**
     * Spring Cloud Stream function entry point. See the class Javadoc for a
     * description of the enrichment pipeline and the {@code Message<byte[]>}
     * signature rationale.
     *
     * @param message the incoming Spring Cloud Stream message; must carry the
     *                {@link KafkaHeaders#RECEIVED_TOPIC} header when it arrived
     *                over the Kafka binder
     * @return zero or more serialized {@link FlowDocumentProtos.FlowDocument}
     *         byte arrays; never {@code null}
     */
    public List<byte[]> processMessage(Message<byte[]> message) {
        if (message == null) {
            return Collections.emptyList();
        }
        byte[] kafkaBytes = message.getPayload();
        if (kafkaBytes == null || kafkaBytes.length == 0) {
            return Collections.emptyList();
        }

        String topicName = message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC, String.class);
        String moduleId = extractModuleId(topicName);
        if (moduleId == null) {
            LOG.debug("Dropping message: unable to extract moduleId from topic '{}'", topicName);
            return Collections.emptyList();
        }

        ProtocolMessageProcessor processor = processorsByModuleId.get(moduleId);
        if (processor == null) {
            LOG.debug("No processor for moduleId '{}', dropping message", moduleId);
            return Collections.emptyList();
        }

        DeserializedSinkMessage deserialized = deserializer.deserialize(moduleId, kafkaBytes);
        if (deserialized == null || deserialized.messageLog().getMessageCount() == 0) {
            return Collections.emptyList();
        }

        TelemetryMessageLog messageLog = deserialized.messageLog();
        List<Flow> flows = processor.process(messageLog);
        if (flows.isEmpty()) {
            return Collections.emptyList();
        }

        // Look up the exporter node info once per message log. The reported
        // source address is the UDP sender on the Minion side, so it maps
        // directly to an ipinterface row in the OpenNMS database.
        String exporterAddress = messageLog.getSourceAddress();
        String location = messageLog.getLocation();
        JdbcNodeInfoLookup.NodeInfo exporterNodeInfo =
                exporterAddress != null && !exporterAddress.isEmpty()
                        ? nodeInfoLookup.lookupByIpAddress(exporterAddress)
                        : null;

        List<byte[]> results = new ArrayList<>(flows.size());
        for (Flow flow : flows) {
            try {
                byte[] serialized = enrichAndSerialize(
                        flow, exporterNodeInfo, exporterAddress, location);
                results.add(serialized);
            } catch (RuntimeException e) {
                LOG.warn("Skipping flow due to enrichment failure ({} flows in batch, moduleId={}): {}",
                        flows.size(), moduleId, e.getMessage(), e);
            }
        }
        return results;
    }

    private byte[] enrichAndSerialize(
            Flow flow,
            JdbcNodeInfoLookup.NodeInfo exporterNodeInfo,
            String exporterAddress,
            String location) {

        String srcAddr = flow.getSrcAddr();
        String dstAddr = flow.getDstAddr();

        JdbcNodeInfoLookup.NodeInfo srcNodeInfo =
                (srcAddr != null && !srcAddr.isEmpty())
                        ? nodeInfoLookup.lookupByIpAddress(srcAddr)
                        : null;
        JdbcNodeInfoLookup.NodeInfo dstNodeInfo =
                (dstAddr != null && !dstAddr.isEmpty())
                        ? nodeInfoLookup.lookupByIpAddress(dstAddr)
                        : null;

        Locality srcLocalityEnum = localityCalculator.classify(srcAddr);
        Locality dstLocalityEnum = localityCalculator.classify(dstAddr);
        Locality flowLocalityEnum = localityCalculator.flowLocality(srcAddr, dstAddr);

        // Mark interfaces on the exporter as flow-enabled. Only run this when
        // the exporter node is known (otherwise nodeId is meaningless) and
        // when the ifindex is present and strictly positive (0 is "unknown"
        // per the Netflow spec).
        String inputIfName = null;
        String outputIfName = null;
        if (exporterNodeInfo != null) {
            Integer inputIfIndex = flow.getInputSnmp();
            if (inputIfIndex != null && inputIfIndex > 0) {
                interfaceMarkingCache.markIfNeeded(exporterNodeInfo.nodeId(), inputIfIndex);
                inputIfName = snmpInterfaceLookup.lookupIfName(exporterNodeInfo.nodeId(), inputIfIndex);
            }
            Integer outputIfIndex = flow.getOutputSnmp();
            if (outputIfIndex != null && outputIfIndex > 0) {
                interfaceMarkingCache.markIfNeeded(exporterNodeInfo.nodeId(), outputIfIndex);
                outputIfName = snmpInterfaceLookup.lookupIfName(exporterNodeInfo.nodeId(), outputIfIndex);
            }
        }

        int dstPort = flow.getDstPort() != null ? flow.getDstPort() : 0;
        int srcPort = flow.getSrcPort() != null ? flow.getSrcPort() : 0;
        int protocol = flow.getProtocol() != null ? flow.getProtocol() : 0;
        String application = applicationClassifier.classify(dstPort, srcPort, protocol);

        FlowDocumentProtos.FlowDocument doc = flowToDocumentMapper.map(
                flow,
                exporterNodeInfo,
                srcNodeInfo,
                dstNodeInfo,
                application,
                localityName(srcLocalityEnum),
                localityName(dstLocalityEnum),
                localityName(flowLocalityEnum),
                exporterAddress,
                location,
                0L,
                inputIfName,
                outputIfName);
        return doc.toByteArray();
    }

    /**
     * Translates a {@link Locality} enum to the string form expected by
     * {@link FlowToDocumentMapper}. The mapper compares to the literals
     * {@code "PRIVATE"} and {@code "PUBLIC"}; anything else maps to
     * {@code LOCALITY_UNKNOWN} in the proto, so we pass {@code null} for the
     * unknown case.
     */
    private static String localityName(Locality locality) {
        if (locality == null || locality == Locality.UNKNOWN) {
            return null;
        }
        return locality.name();
    }

    private static String extractModuleId(String topicName) {
        if (topicName == null) {
            return null;
        }
        if (topicName.startsWith(DELTAV_SINK_PREFIX)) {
            return topicName.substring(DELTAV_SINK_PREFIX.length());
        }
        return null;
    }
}
