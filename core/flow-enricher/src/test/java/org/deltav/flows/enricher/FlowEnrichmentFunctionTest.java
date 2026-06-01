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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.deltav.flows.enricher.classification.ApplicationClassifier;
import org.deltav.flows.enricher.enrichment.FlowLocalityCalculator;
import org.deltav.flows.enricher.enrichment.FlowLocalityCalculator.Locality;
import org.deltav.flows.enricher.enrichment.InterfaceMarkingCache;
import org.deltav.flows.enricher.enrichment.JdbcNodeInfoLookup;
import org.deltav.flows.enricher.enrichment.JdbcSnmpInterfaceLookup;
import org.deltav.flows.enricher.mapping.FlowToDocumentMapper;
import org.deltav.flows.enricher.protocol.ProtocolMessageProcessor;
import org.deltav.flows.proto.FlowDocumentProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.core.ipc.sink.model.SinkMessage;
import org.opennms.integration.api.v1.flows.Flow.NetflowVersion;
import org.opennms.netmgt.flows.api.Flow;
import org.opennms.netmgt.telemetry.common.ipc.TelemetryProtos;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

class FlowEnrichmentFunctionTest {

    private static final String EXPORTER_ADDRESS = "192.0.2.10";
    private static final String MINION_LOCATION = "Default";
    private static final String NF5_TOPIC = "DeltaV.Sink.Telemetry-Netflow-5";
    private static final String NF5_MODULE_ID = "Telemetry-Netflow-5";

    private SinkMessageDeserializer deserializer;
    private JdbcNodeInfoLookup nodeInfoLookup;
    private FlowLocalityCalculator localityCalculator;
    private InterfaceMarkingCache interfaceMarkingCache;
    private ApplicationClassifier applicationClassifier;
    private FlowToDocumentMapper flowToDocumentMapper;
    private JdbcSnmpInterfaceLookup snmpInterfaceLookup;
    private ProtocolMessageProcessor nf5Processor;

    private FlowEnrichmentFunction function;

    @BeforeEach
    void setUp() {
        deserializer = new SinkMessageDeserializer();
        nodeInfoLookup = mock(JdbcNodeInfoLookup.class);
        localityCalculator = mock(FlowLocalityCalculator.class);
        interfaceMarkingCache = mock(InterfaceMarkingCache.class);
        applicationClassifier = mock(ApplicationClassifier.class);
        flowToDocumentMapper = new FlowToDocumentMapper();
        snmpInterfaceLookup = mock(JdbcSnmpInterfaceLookup.class);
        nf5Processor = mock(ProtocolMessageProcessor.class);

        // Default mock behavior: unknown locality for unknown addresses,
        // null node lookups, "unknown" application classification.
        when(localityCalculator.classify(anyString())).thenReturn(Locality.UNKNOWN);
        when(localityCalculator.flowLocality(anyString(), anyString())).thenReturn(Locality.UNKNOWN);
        when(applicationClassifier.classify(anyInt(), anyInt(), anyInt())).thenReturn("unknown");

        function = new FlowEnrichmentFunction(
                deserializer,
                nodeInfoLookup,
                localityCalculator,
                interfaceMarkingCache,
                applicationClassifier,
                flowToDocumentMapper,
                snmpInterfaceLookup,
                Map.of(NF5_MODULE_ID, nf5Processor));
    }

    // ---- Commit 3 carry-forward tests (updated for Message<byte[]> signature) ----

    @Test
    void returnsEmptyListForNullMessage() {
        List<byte[]> result = function.processMessage(null);
        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyListForEmptyPayload() {
        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, new byte[0]));
        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyListForGarbageBytes() {
        Message<byte[]> message = messageWithTopic(NF5_TOPIC, new byte[]{0x01, 0x02, 0x03, 0x04});
        List<byte[]> result = function.processMessage(message);
        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyListForEmptyTelemetryMessageLog() {
        // Valid SinkMessage envelope wrapping an empty TelemetryMessageLog (no
        // flow records): function must drop the message without producing output.
        byte[] payload = TelemetryProtos.TelemetryMessageLog.newBuilder()
                .setLocation(MINION_LOCATION)
                .setSystemId("minion-01")
                .setSourceAddress(EXPORTER_ADDRESS)
                .setSourcePort(4729)
                .build()
                .toByteArray();
        byte[] kafkaBytes = buildSinkMessageBytes("test-empty", payload);

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyListForUnknownModuleId() {
        // Topic with prefix we recognize but an unknown module ID suffix.
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(
                messageWithTopic("DeltaV.Sink.Telemetry-Made-Up", kafkaBytes));

        assertThat(result).isEmpty();
    }

    // ---- New Commit 5 full-pipeline tests ----

    @Test
    void dispatchesByTopicPrefixAndExtractsModuleId() {
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 54321, 443, 6, 1, 2);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).hasSize(1);
        verify(nf5Processor).process(any());
    }

    @Test
    void handlesDeltavSinkPrefixToo() {
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 54321, 443, 6, 1, 2);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(
                messageWithTopic("DeltaV.Sink." + NF5_MODULE_ID, kafkaBytes));

        assertThat(result).hasSize(1);
    }

    @Test
    void returnsEmptyListForUnknownTopicPrefix() {
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(
                messageWithTopic("Custom.Sink." + NF5_MODULE_ID, kafkaBytes));

        assertThat(result).isEmpty();
        verify(nf5Processor, never()).process(any());
    }

    @Test
    void returnsEmptyListWhenTopicHeaderMissing() {
        byte[] kafkaBytes = buildValidSinkMessageBytes();
        Message<byte[]> noHeaderMessage = MessageBuilder.withPayload(kafkaBytes).build();

        List<byte[]> result = function.processMessage(noHeaderMessage);

        assertThat(result).isEmpty();
        verify(nf5Processor, never()).process(any());
    }

    @Test
    void looksUpExporterNodeOnceAndReusesAcrossFlows() {
        Flow flow1 = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 1, 2);
        Flow flow2 = buildFlow("10.0.0.3", "10.0.0.4", 333, 444, 17, 3, 4);
        Flow flow3 = buildFlow("10.0.0.5", "10.0.0.6", 555, 666, 6, 5, 6);
        when(nf5Processor.process(any())).thenReturn(List.of(flow1, flow2, flow3));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).hasSize(3);
        // Exporter looked up exactly once regardless of how many flows we had.
        verify(nodeInfoLookup, times(1)).lookupByIpAddress(EXPORTER_ADDRESS);
    }

    @Test
    void enrichesEachFlowWithSrcAndDstNodeLookups() {
        Flow flow1 = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 1, 2);
        Flow flow2 = buildFlow("10.0.0.3", "10.0.0.4", 333, 444, 17, 3, 4);
        when(nf5Processor.process(any())).thenReturn(List.of(flow1, flow2));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        verify(nodeInfoLookup).lookupByIpAddress("10.0.0.1");
        verify(nodeInfoLookup).lookupByIpAddress("10.0.0.2");
        verify(nodeInfoLookup).lookupByIpAddress("10.0.0.3");
        verify(nodeInfoLookup).lookupByIpAddress("10.0.0.4");
    }

    @Test
    void callsInterfaceMarkingForInputAndOutputIfindex() {
        // Exporter node resolves, flow has non-zero input/output ifindex.
        JdbcNodeInfoLookup.NodeInfo exporterInfo =
                new JdbcNodeInfoLookup.NodeInfo(42, "Minions", "exporter-1", MINION_LOCATION, "exporter-1-label");
        when(nodeInfoLookup.lookupByIpAddress(EXPORTER_ADDRESS)).thenReturn(exporterInfo);
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 7, 11);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        verify(interfaceMarkingCache).markIfNeeded(42L, 7);
        verify(interfaceMarkingCache).markIfNeeded(42L, 11);
    }

    @Test
    void skipsInterfaceMarkingWhenExporterUnknown() {
        // Exporter lookup returns null — no node ID available.
        when(nodeInfoLookup.lookupByIpAddress(EXPORTER_ADDRESS)).thenReturn(null);
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 7, 11);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        verify(interfaceMarkingCache, never()).markIfNeeded(anyLong(), anyInt());
    }

    @Test
    void skipsInterfaceMarkingWhenIfindexIsZeroOrNull() {
        JdbcNodeInfoLookup.NodeInfo exporterInfo =
                new JdbcNodeInfoLookup.NodeInfo(99, "Minions", "exporter-9", MINION_LOCATION, "exporter-9-label");
        when(nodeInfoLookup.lookupByIpAddress(EXPORTER_ADDRESS)).thenReturn(exporterInfo);
        // Ifindex 0 for both directions: "unknown" per Netflow spec.
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 0, 0);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        verify(interfaceMarkingCache, never()).markIfNeeded(anyLong(), anyInt());
    }

    @Test
    void populatesApplicationFieldFromClassifier() throws InvalidProtocolBufferException {
        when(applicationClassifier.classify(443, 54321, 6)).thenReturn("HTTPS");
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 54321, 443, 6, 1, 2);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).hasSize(1);
        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(result.get(0));
        assertThat(doc.getApplication()).isEqualTo("HTTPS");
    }

    @Test
    void populatesLocalityFieldsFromCalculator() throws InvalidProtocolBufferException {
        when(localityCalculator.classify("10.0.0.1")).thenReturn(Locality.PRIVATE);
        when(localityCalculator.classify("198.51.100.5")).thenReturn(Locality.PUBLIC);
        when(localityCalculator.flowLocality("10.0.0.1", "198.51.100.5")).thenReturn(Locality.PUBLIC);
        Flow flow = buildFlow("10.0.0.1", "198.51.100.5", 54321, 443, 6, 1, 2);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).hasSize(1);
        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(result.get(0));
        assertThat(doc.getSrcLocality()).isEqualTo(FlowDocumentProtos.Locality.PRIVATE);
        assertThat(doc.getDstLocality()).isEqualTo(FlowDocumentProtos.Locality.PUBLIC);
        assertThat(doc.getFlowLocality()).isEqualTo(FlowDocumentProtos.Locality.PUBLIC);
    }

    @Test
    void continuesProcessingWhenOneFlowFailsEnrichment() {
        // Construct one flow that throws when getSrcAddr() is called, and one
        // that doesn't. The failing flow must be skipped but the good one must
        // still produce an output record.
        Flow goodFlow = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 1, 2);
        Flow badFlow = mock(Flow.class);
        when(badFlow.getSrcAddr()).thenThrow(new RuntimeException("simulated enrichment failure"));
        when(nf5Processor.process(any())).thenReturn(List.of(badFlow, goodFlow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        // Exactly one result from the surviving good flow.
        assertThat(result).hasSize(1);
    }

    @Test
    void populatesHostAndLocationFromTelemetryMessageLog() throws InvalidProtocolBufferException {
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 1, 2);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).hasSize(1);
        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(result.get(0));
        assertThat(doc.getHost()).isEqualTo(EXPORTER_ADDRESS);
        assertThat(doc.getLocation()).isEqualTo(MINION_LOCATION);
    }

    @Test
    void passesZeroClockCorrectionInPhase15() throws InvalidProtocolBufferException {
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 1, 2);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).hasSize(1);
        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(result.get(0));
        assertThat(doc.getClockCorrection()).isZero();
    }

    @Test
    void populatesExporterNodeInfoFromLookup() throws InvalidProtocolBufferException {
        JdbcNodeInfoLookup.NodeInfo exporterInfo =
                new JdbcNodeInfoLookup.NodeInfo(77, "Minions", "exporter-77", MINION_LOCATION, "exporter-77-label");
        when(nodeInfoLookup.lookupByIpAddress(EXPORTER_ADDRESS)).thenReturn(exporterInfo);
        Flow flow = buildFlow("10.0.0.1", "10.0.0.2", 111, 222, 6, 1, 2);
        when(nf5Processor.process(any())).thenReturn(List.of(flow));
        byte[] kafkaBytes = buildValidSinkMessageBytes();

        List<byte[]> result = function.processMessage(messageWithTopic(NF5_TOPIC, kafkaBytes));

        assertThat(result).hasSize(1);
        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(result.get(0));
        assertThat(doc.hasExporterNode()).isTrue();
        assertThat(doc.getExporterNode().getNodeId()).isEqualTo(77);
        assertThat(doc.getExporterNode().getForeignSource()).isEqualTo("Minions");
        assertThat(doc.getExporterNode().getForeignId()).isEqualTo("exporter-77");
    }

    // ---- Helpers ----

    private static Message<byte[]> messageWithTopic(String topicName, byte[] payload) {
        return MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, topicName)
                .build();
    }

    private static byte[] buildValidSinkMessageBytes() {
        // A valid, non-empty TelemetryMessageLog wrapped in a SinkMessage
        // envelope. The actual contents of the TelemetryMessage entries don't
        // matter because the processor is mocked.
        byte[] payload = TelemetryProtos.TelemetryMessageLog.newBuilder()
                .setLocation(MINION_LOCATION)
                .setSystemId("minion-01")
                .setSourceAddress(EXPORTER_ADDRESS)
                .setSourcePort(2055)
                .addMessage(TelemetryProtos.TelemetryMessage.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .setBytes(ByteString.copyFrom(new byte[]{0x00, 0x01, 0x02, 0x03}))
                        .build())
                .build()
                .toByteArray();
        return buildSinkMessageBytes("test-msg", payload);
    }

    private static byte[] buildSinkMessageBytes(String messageId, byte[] payload) {
        return SinkMessage.newBuilder()
                .setMessageId(messageId)
                .setContent(ByteString.copyFrom(payload))
                .setCurrentChunkNumber(0)
                .setTotalChunks(1)
                .build()
                .toByteArray();
    }

    /**
     * Builds a minimal {@link Flow} mock that returns the supplied addressing
     * fields and otherwise null-valued/default returns for everything else.
     * The {@link FlowToDocumentMapper} tolerates all-null wrapper-typed
     * getters per its null-wrapper discipline.
     */
    private static Flow buildFlow(
            String srcAddr,
            String dstAddr,
            int srcPort,
            int dstPort,
            int protocol,
            int inputSnmp,
            int outputSnmp) {
        Flow flow = mock(Flow.class);
        when(flow.getSrcAddr()).thenReturn(srcAddr);
        when(flow.getDstAddr()).thenReturn(dstAddr);
        when(flow.getSrcPort()).thenReturn(srcPort);
        when(flow.getDstPort()).thenReturn(dstPort);
        when(flow.getProtocol()).thenReturn(protocol);
        when(flow.getInputSnmp()).thenReturn(inputSnmp);
        when(flow.getOutputSnmp()).thenReturn(outputSnmp);
        when(flow.getTimestamp()).thenReturn(Instant.now());
        when(flow.getNetflowVersion()).thenReturn(NetflowVersion.V5);
        when(flow.getSrcAddrHostname()).thenReturn(Optional.empty());
        when(flow.getDstAddrHostname()).thenReturn(Optional.empty());
        when(flow.getNextHopHostname()).thenReturn(Optional.empty());
        // Primitive long/int getters return 0 by default, which is fine.
        return flow;
    }
}
