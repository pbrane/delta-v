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
package org.deltav.flows.enricher.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;

import org.deltav.flows.enricher.FlowEnricherApplication;
import org.deltav.flows.enricher.enrichment.InterfaceMarkingCache;
import org.deltav.flows.enricher.parser.ThreadLocalDispatcher;
import org.deltav.nodecontext.NodeContextCache;
import org.deltav.flows.proto.FlowDocumentProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.core.ipc.sink.model.SinkMessage;
import org.opennms.netmgt.telemetry.api.receiver.TelemetryMessage;
import org.opennms.netmgt.telemetry.common.ipc.TelemetryProtos;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.IpfixUdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.Netflow5UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.Netflow9UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.transport.FlowMessage;
import org.opennms.netmgt.telemetry.protocols.netflow.transport.NetflowVersion;
import org.opennms.netmgt.telemetry.protocols.sflow.parser.SFlowUdpParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import io.netty.buffer.ByteBuf;

/**
 * End-to-end integration test for the flow-enricher Spring Cloud Stream
 * pipeline. Loads the full {@link FlowEnricherApplication} Spring Boot context
 * with the in-memory {@link TestChannelBinderConfiguration test binder},
 * synthesizes Sink messages for each of the four supported flow protocols
 * (Netflow-5, Netflow-9, IPFIX, sFlow), and verifies that enriched
 * {@link FlowDocumentProtos.FlowDocument} records are emitted on the
 * {@code deltav-flows} output destination.
 *
 * <p>The {@link NodeContextCache} and {@link InterfaceMarkingCache} beans
 * are replaced with Mockito mocks via {@link MockitoBean} (Spring Boot 4.0's
 * successor to {@code @MockBean}). This sidesteps the
 * {@link javax.sql.DataSource} dependency chain in
 * {@code FlowEnricherConfiguration}: with {@code InterfaceMarkingCache} mocked,
 * Spring never needs to instantiate {@code flowEnricherJdbcTemplate}, so no
 * {@code DataSource} bean is ever requested. We still explicitly exclude
 * {@code DataSourceAutoConfiguration} for defense in depth.
 *
 * <p>The Netflow5/Netflow9/IPFIX {@link org.opennms.netmgt.telemetry.listeners.UdpParser}
 * beans are replaced with Mockito mocks that dispatch the received
 * {@link ByteBuf} bytes unchanged via the shared {@link ThreadLocalDispatcher}.
 * Stage 2 of {@code AbstractProtocolMessageProcessor} therefore sees the
 * pre-encoded {@code FlowMessage} protobuf bytes the test put into the Sink
 * message envelope, bypassing wire-format parsing.
 *
 * <p>{@link SFlowUdpParser} is deliberately NOT mocked — it is wired as a
 * real Spring bean. This means the test exercises the delta-v-local
 * {@link org.opennms.core.concurrent.LogPreservingThreadFactory} shim
 * transitively via bean construction and {@code flowParserLifecycle} start,
 * giving unit-test-speed coverage of a class that previously required a
 * full container rebuild to validate. The sFlow parser sits idle during
 * this IT because none of the tests feed raw sFlow wire bytes; its Stage 1
 * ↔ Stage 2 BSON handoff is covered by {@code SFlowMessageProcessorTest}
 * using a {@code FakeUdpParser}. Live sFlow verification via a real
 * wire-format exporter is a separate followup.
 *
 * <p>Because {@link NodeContextCache#getByIp(String)} always returns
 * {@link java.util.Optional#empty()} in this test, the enricher's per-flow
 * logic runs but cannot populate the {@code exporter_node}/{@code src_node}/
 * {@code dest_node} fields. We therefore assert on the scalar fields the
 * enricher populates regardless of node lookup success: {@code src_address},
 * {@code dst_address}, {@code netflow_version}, {@code host}, {@code location},
 * and {@code application}.
 */
@SpringBootTest(
        classes = { FlowEnricherApplication.class },
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.cloud.function.definition=enrichFlows",
                "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
                // Force the in-memory TestChannelBinder ("integration") to be the
                // default binder even though the Kafka binder jar is also on the
                // classpath. Without this, SCS will try to instantiate the Kafka
                // binder and fail because there's no broker.
                "spring.cloud.stream.defaultBinder=integration",
                // Exclude the shared NodeContextConsumerAutoConfiguration so
                // NodeContextKafkaBootstrap never starts a real consumer thread
                // trying to reach kafka:9092. The NodeContextCache bean this IT
                // needs is supplied by the @MockitoBean below.
                "spring.autoconfigure.exclude=org.deltav.nodecontext.NodeContextConsumerAutoConfiguration",
                // The function returns List<byte[]>. Without useNativeEncoding,
                // Spring Cloud Function JSON-serializes the entire list into a
                // single "[]"-shaped payload; with it, the binder honors the
                // splitter semantics and emits one output message per list
                // element. Matches the production setting in application.yml.
                "spring.cloud.stream.bindings.enrichFlows-out-0.producer.use-native-encoding=true"
        })
@Import(TestChannelBinderConfiguration.class)
class FlowEnrichmentStreamBinderIT {

    private static final String OUTPUT_DESTINATION = "deltav-flows";
    private static final String MINION_LOCATION = "test-location";
    private static final String EXPORTER_ADDRESS = "192.0.2.254";

    // Destination names matching the four comma-separated entries in
    // application.yml's enrichFlows-in-0.destination. The test binder
    // provisions one SubscribableChannel per entry, keyed by
    // "<destination>.destination".
    private static final String NF5_DESTINATION = "DeltaV.Sink.Telemetry-Netflow-5";
    private static final String NF9_DESTINATION = "DeltaV.Sink.Telemetry-Netflow-9";
    private static final String IPFIX_DESTINATION = "DeltaV.Sink.Telemetry-IPFIX";
    // SFLOW_DESTINATION omitted because this IT does not drive raw sFlow
    // wire bytes through the enricher. The real SFlowUdpParser bean is
    // constructed in this context (via the LogPreservingThreadFactory shim)
    // but sits idle; its Stage 1 ↔ Stage 2 BSON handoff is covered by
    // SFlowMessageProcessorTest with a FakeUdpParser.

    @Autowired
    private InputDestination input;

    @Autowired
    private OutputDestination output;

    @Autowired
    private ThreadLocalDispatcher threadLocalDispatcher;

    @MockitoBean
    private NodeContextCache nodeContextCache;

    @MockitoBean
    private InterfaceMarkingCache interfaceMarkingCache;

    /**
     * Mock parsers replace the real Netflow5/9/IPFIX UdpParser beans.
     * Their {@code parse()} stubs dispatch received buffer bytes unchanged
     * via {@link ThreadLocalDispatcher} so Stage 2 sees the same
     * pre-parsed bytes the test injected (FlowMessage protobuf). sFlow uses
     * its real bean (see class-level javadoc for rationale).
     */
    @MockitoBean
    private Netflow5UdpParser netflow5UdpParser;

    @MockitoBean
    private Netflow9UdpParser netflow9UdpParser;

    @MockitoBean
    private IpfixUdpParser ipfixUdpParser;

    @BeforeEach
    void setUp() throws Exception {
        // All cache lookups return empty so the enricher runs without node info.
        when(nodeContextCache.getByIp(anyString())).thenReturn(java.util.Optional.empty());
        when(nodeContextCache.ifName(anyInt(), anyInt())).thenReturn(java.util.Optional.empty());

        // Wire each mock parser to act as a passthrough: dispatch the received
        // ByteBuf's bytes directly to the ThreadLocalDispatcher so that Stage 2
        // of AbstractProtocolMessageProcessor sees the pre-parsed bytes.
        configurePassthroughParser(netflow5UdpParser);
        configurePassthroughParser(netflow9UdpParser);
        configurePassthroughParser(ipfixUdpParser);

        // Drain any residual messages from previous tests on the shared
        // output destination.
        output.clear(OUTPUT_DESTINATION);
    }

    /**
     * Stubs {@code parser.parse(buf, remote, local)} to read all bytes from
     * {@code buf}, wrap them in a {@link TelemetryMessage}, and dispatch to
     * the currently-installed {@link ThreadLocalDispatcher} delegate.
     */
    private void configurePassthroughParser(
            org.opennms.netmgt.telemetry.listeners.UdpParser parser) throws Exception {
        doAnswer(invocation -> {
            ByteBuf buf = invocation.getArgument(0);
            InetSocketAddress remoteAddress = invocation.getArgument(1);
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            TelemetryMessage msg = new TelemetryMessage(remoteAddress, ByteBuffer.wrap(bytes));
            threadLocalDispatcher.send(msg);
            return CompletableFuture.completedFuture(null);
        }).when(parser).parse(any(ByteBuf.class), any(InetSocketAddress.class), any(InetSocketAddress.class));
    }

    @Test
    void netflow5MessageProducesEnrichedFlowDocument() throws Exception {
        FlowMessage netflow5 = FlowMessage.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setNetflowVersion(NetflowVersion.V5)
                .setSrcAddress("192.0.2.1")
                .setDstAddress("198.51.100.2")
                .setNextHopAddress("203.0.113.1")
                .setSrcPort(UInt32Value.of(54321))
                .setDstPort(UInt32Value.of(443))
                .setProtocol(UInt32Value.of(6))
                .setTcpFlags(UInt32Value.of(0x18))
                .setNumBytes(UInt64Value.of(1500L))
                .setNumPackets(UInt64Value.of(10L))
                .setFirstSwitched(UInt64Value.of(1_700_000_000_000L))
                .setLastSwitched(UInt64Value.of(1_700_000_001_000L))
                .setDeltaSwitched(UInt64Value.of(1_700_000_000_000L))
                .setInputSnmpIfindex(UInt32Value.of(1))
                .setOutputSnmpIfindex(UInt32Value.of(2))
                .setIpProtocolVersion(UInt32Value.of(4))
                .build();
        byte[] sinkBytes = buildSinkMessageBytes(buildTelemetryMessageLog(netflow5.toByteArray()));

        send(NF5_DESTINATION, sinkBytes);

        Message<byte[]> received = output.receive(10_000, OUTPUT_DESTINATION);
        assertThat(received).as("expected a FlowDocument on %s", OUTPUT_DESTINATION).isNotNull();

        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(received.getPayload());
        assertThat(doc.getNetflowVersion()).isEqualTo(FlowDocumentProtos.NetflowVersion.V5);
        assertThat(doc.getSrcAddress()).isEqualTo("192.0.2.1");
        assertThat(doc.getDstAddress()).isEqualTo("198.51.100.2");
        assertThat(doc.getDstPort().getValue()).isEqualTo(443);
        assertThat(doc.getProtocol().getValue()).isEqualTo(6);
        assertThat(doc.getHost()).isEqualTo(EXPORTER_ADDRESS);
        assertThat(doc.getLocation()).isEqualTo(MINION_LOCATION);
        assertThat(doc.getApplication()).isEqualTo("HTTPS");
        // Node lookups all returned null, so exporter/src/dst node fields are
        // unset (hasExporterNode / hasSrcNode / hasDestNode == false).
        assertThat(doc.hasExporterNode()).isFalse();
        assertThat(doc.hasSrcNode()).isFalse();
        assertThat(doc.hasDestNode()).isFalse();
    }

    @Test
    void netflow9MessageProducesEnrichedFlowDocument() throws Exception {
        FlowMessage netflow9 = FlowMessage.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setNetflowVersion(NetflowVersion.V9)
                .setSrcAddress("10.0.0.1")
                .setDstAddress("10.0.0.2")
                .setNextHopAddress("10.0.0.254")
                .setSrcPort(UInt32Value.of(12345))
                .setDstPort(UInt32Value.of(53))
                .setProtocol(UInt32Value.of(17)) // UDP
                .setNumBytes(UInt64Value.of(2048L))
                .setNumPackets(UInt64Value.of(16L))
                .setFirstSwitched(UInt64Value.of(1_700_000_000_000L))
                .setLastSwitched(UInt64Value.of(1_700_000_001_000L))
                .setDeltaSwitched(UInt64Value.of(1_700_000_000_000L))
                .setInputSnmpIfindex(UInt32Value.of(3))
                .setOutputSnmpIfindex(UInt32Value.of(4))
                .setIpProtocolVersion(UInt32Value.of(4))
                .build();
        byte[] sinkBytes = buildSinkMessageBytes(buildTelemetryMessageLog(netflow9.toByteArray()));

        send(NF9_DESTINATION, sinkBytes);

        Message<byte[]> received = output.receive(10_000, OUTPUT_DESTINATION);
        assertThat(received).as("expected a FlowDocument on %s", OUTPUT_DESTINATION).isNotNull();

        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(received.getPayload());
        assertThat(doc.getNetflowVersion()).isEqualTo(FlowDocumentProtos.NetflowVersion.V9);
        assertThat(doc.getSrcAddress()).isEqualTo("10.0.0.1");
        assertThat(doc.getDstAddress()).isEqualTo("10.0.0.2");
        assertThat(doc.getDstPort().getValue()).isEqualTo(53);
        assertThat(doc.getProtocol().getValue()).isEqualTo(17);
        assertThat(doc.getHost()).isEqualTo(EXPORTER_ADDRESS);
        assertThat(doc.getLocation()).isEqualTo(MINION_LOCATION);
        assertThat(doc.getApplication()).isEqualTo("DNS");
    }

    @Test
    void ipfixMessageProducesEnrichedFlowDocument() throws Exception {
        FlowMessage ipfix = FlowMessage.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setNetflowVersion(NetflowVersion.IPFIX)
                .setSrcAddress("2001:db8::1")
                .setDstAddress("2001:db8::2")
                .setNextHopAddress("2001:db8::ffff")
                .setSrcPort(UInt32Value.of(40000))
                .setDstPort(UInt32Value.of(22))
                .setProtocol(UInt32Value.of(6)) // TCP
                .setTcpFlags(UInt32Value.of(0x02))
                .setNumBytes(UInt64Value.of(4096L))
                .setNumPackets(UInt64Value.of(32L))
                .setFirstSwitched(UInt64Value.of(1_700_000_000_000L))
                .setLastSwitched(UInt64Value.of(1_700_000_002_000L))
                .setDeltaSwitched(UInt64Value.of(1_700_000_000_000L))
                .setInputSnmpIfindex(UInt32Value.of(5))
                .setOutputSnmpIfindex(UInt32Value.of(6))
                .setIpProtocolVersion(UInt32Value.of(6))
                .build();
        byte[] sinkBytes = buildSinkMessageBytes(buildTelemetryMessageLog(ipfix.toByteArray()));

        send(IPFIX_DESTINATION, sinkBytes);

        Message<byte[]> received = output.receive(10_000, OUTPUT_DESTINATION);
        assertThat(received).as("expected a FlowDocument on %s", OUTPUT_DESTINATION).isNotNull();

        FlowDocumentProtos.FlowDocument doc = FlowDocumentProtos.FlowDocument.parseFrom(received.getPayload());
        assertThat(doc.getNetflowVersion()).isEqualTo(FlowDocumentProtos.NetflowVersion.IPFIX);
        assertThat(doc.getSrcAddress()).isEqualTo("2001:db8::1");
        assertThat(doc.getDstAddress()).isEqualTo("2001:db8::2");
        assertThat(doc.getDstPort().getValue()).isEqualTo(22);
        assertThat(doc.getProtocol().getValue()).isEqualTo(6);
        assertThat(doc.getIpProtocolVersion().getValue()).isEqualTo(6);
        assertThat(doc.getHost()).isEqualTo(EXPORTER_ADDRESS);
        assertThat(doc.getLocation()).isEqualTo(MINION_LOCATION);
        assertThat(doc.getApplication()).isEqualTo("SSH");
    }

    @Test
    void unknownTopicPrefixDropsMessage() throws Exception {
        // Use a destination the enricher is NOT bound to: the input side is
        // bound to the four DeltaV.Sink.* channels, so to exercise the
        // "unknown prefix" branch we must send through one of the bound
        // destinations but with a header that carries a prefix the enricher
        // doesn't recognize. We do that by overriding RECEIVED_TOPIC on the
        // outbound message.
        FlowMessage netflow5 = buildSimpleNetflow5();
        byte[] sinkBytes = buildSinkMessageBytes(buildTelemetryMessageLog(netflow5.toByteArray()));

        Message<byte[]> msg = MessageBuilder.withPayload(sinkBytes)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, "Custom.Sink.Telemetry-Netflow-5")
                .build();
        input.send(msg, NF5_DESTINATION);

        // Enricher should drop the message because "Custom.Sink." isn't a
        // recognized prefix. Short timeout since we're asserting the absence
        // of output.
        Message<byte[]> received = output.receive(1_000, OUTPUT_DESTINATION);
        assertThat(received).as("expected no output for unknown topic prefix").isNull();
    }

    // ---- helpers ----

    /**
     * Wraps a payload in a Spring {@link Message} with the Kafka
     * {@code RECEIVED_TOPIC} header set to the destination name (this
     * simulates the header a real Kafka binder would attach on inbound
     * records) and sends it through the {@link InputDestination} binding
     * named after the destination.
     */
    private void send(String destinationName, byte[] sinkBytes) {
        Message<byte[]> msg = MessageBuilder.withPayload(sinkBytes)
                .setHeader(KafkaHeaders.RECEIVED_TOPIC, destinationName)
                .build();
        input.send(msg, destinationName);
    }

    private static TelemetryProtos.TelemetryMessageLog buildTelemetryMessageLog(byte[] entryBytes) {
        return TelemetryProtos.TelemetryMessageLog.newBuilder()
                .setLocation(MINION_LOCATION)
                .setSystemId("test-system")
                .setSourceAddress(EXPORTER_ADDRESS)
                .setSourcePort(2055)
                .addMessage(TelemetryProtos.TelemetryMessage.newBuilder()
                        .setBytes(ByteString.copyFrom(entryBytes))
                        .setTimestamp(System.currentTimeMillis())
                        .build())
                .build();
    }

    private static byte[] buildSinkMessageBytes(TelemetryProtos.TelemetryMessageLog messageLog) {
        return SinkMessage.newBuilder()
                .setMessageId("it-" + System.nanoTime())
                .setContent(ByteString.copyFrom(messageLog.toByteArray()))
                .setCurrentChunkNumber(0)
                .setTotalChunks(1)
                .build()
                .toByteArray();
    }

    private static FlowMessage buildSimpleNetflow5() {
        return FlowMessage.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setNetflowVersion(NetflowVersion.V5)
                .setSrcAddress("192.0.2.1")
                .setDstAddress("198.51.100.2")
                .setSrcPort(UInt32Value.of(54321))
                .setDstPort(UInt32Value.of(443))
                .setProtocol(UInt32Value.of(6))
                .setNumBytes(UInt64Value.of(1500L))
                .setNumPackets(UInt64Value.of(10L))
                .setFirstSwitched(UInt64Value.of(1_700_000_000_000L))
                .setLastSwitched(UInt64Value.of(1_700_000_001_000L))
                .setDeltaSwitched(UInt64Value.of(1_700_000_000_000L))
                .setInputSnmpIfindex(UInt32Value.of(1))
                .setOutputSnmpIfindex(UInt32Value.of(2))
                .setIpProtocolVersion(UInt32Value.of(4))
                .build();
    }

}
