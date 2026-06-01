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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;

import org.deltav.flows.enricher.enrichment.JdbcNodeInfoLookup;
import org.deltav.flows.proto.FlowDocumentProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.integration.api.v1.flows.Flow.Direction;
import org.opennms.integration.api.v1.flows.Flow.NetflowVersion;
import org.opennms.integration.api.v1.flows.Flow.SamplingAlgorithm;
import org.opennms.netmgt.flows.api.Flow;

class FlowToDocumentMapperTest {

    private FlowToDocumentMapper mapper;
    private Flow flow;

    @BeforeEach
    void setUp() {
        mapper = new FlowToDocumentMapper();
        flow = mock(Flow.class);
        // Sensible defaults so the mock returns null/0 everywhere unless overridden.
        when(flow.getSrcAddrHostname()).thenReturn(Optional.empty());
        when(flow.getDstAddrHostname()).thenReturn(Optional.empty());
        when(flow.getNextHopHostname()).thenReturn(Optional.empty());
    }

    @Test
    void mapsTimestampAndScalars() {
        when(flow.getTimestamp()).thenReturn(Instant.ofEpochMilli(1_700_000_000_000L));
        when(flow.getSrcAddr()).thenReturn("10.0.0.1");
        when(flow.getDstAddr()).thenReturn("10.0.0.2");
        when(flow.getNextHop()).thenReturn("10.0.0.254");
        when(flow.getSrcAddrHostname()).thenReturn(Optional.of("src.local"));
        when(flow.getDstAddrHostname()).thenReturn(Optional.of("dst.local"));
        when(flow.getNextHopHostname()).thenReturn(Optional.of("gw.local"));
        when(flow.getVlan()).thenReturn(42);

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.getTimestamp()).isEqualTo(1_700_000_000_000L);
        assertThat(doc.getSrcAddress()).isEqualTo("10.0.0.1");
        assertThat(doc.getDstAddress()).isEqualTo("10.0.0.2");
        assertThat(doc.getNextHopAddress()).isEqualTo("10.0.0.254");
        assertThat(doc.getSrcHostname()).isEqualTo("src.local");
        assertThat(doc.getDstHostname()).isEqualTo("dst.local");
        assertThat(doc.getNextHopHostname()).isEqualTo("gw.local");
        assertThat(doc.getVlan()).isEqualTo("42");
    }

    @Test
    void omitsUInt64ValueWrapperFieldsWhenFlowGetterReturnsNull() {
        when(flow.getBytes()).thenReturn(null);
        when(flow.getPackets()).thenReturn(null);
        when(flow.getDstAs()).thenReturn(null);
        when(flow.getSrcAs()).thenReturn(null);
        when(flow.getFirstSwitched()).thenReturn(null);
        when(flow.getLastSwitched()).thenReturn(null);
        when(flow.getDeltaSwitched()).thenReturn(null);

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasNumBytes()).isFalse();
        assertThat(doc.hasNumPackets()).isFalse();
        assertThat(doc.hasDstAs()).isFalse();
        assertThat(doc.hasSrcAs()).isFalse();
        assertThat(doc.hasFirstSwitched()).isFalse();
        assertThat(doc.hasLastSwitched()).isFalse();
        assertThat(doc.hasDeltaSwitched()).isFalse();
    }

    @Test
    void populatesUInt64ValueWrapperFieldsWhenFlowGetterReturnsZero() {
        when(flow.getBytes()).thenReturn(0L);
        when(flow.getPackets()).thenReturn(0L);
        when(flow.getDstAs()).thenReturn(0L);
        when(flow.getSrcAs()).thenReturn(0L);
        when(flow.getFirstSwitched()).thenReturn(Instant.EPOCH);
        when(flow.getLastSwitched()).thenReturn(Instant.EPOCH);
        when(flow.getDeltaSwitched()).thenReturn(Instant.EPOCH);

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasNumBytes()).isTrue();
        assertThat(doc.getNumBytes().getValue()).isEqualTo(0L);
        assertThat(doc.hasNumPackets()).isTrue();
        assertThat(doc.getNumPackets().getValue()).isEqualTo(0L);
        assertThat(doc.hasDstAs()).isTrue();
        assertThat(doc.hasSrcAs()).isTrue();
        assertThat(doc.hasFirstSwitched()).isTrue();
        assertThat(doc.getFirstSwitched().getValue()).isEqualTo(0L);
        assertThat(doc.hasLastSwitched()).isTrue();
        assertThat(doc.hasDeltaSwitched()).isTrue();
    }

    @Test
    void populatesExporterNodeWhenEnrichmentProvidesIt() {
        JdbcNodeInfoLookup.NodeInfo exporter = new JdbcNodeInfoLookup.NodeInfo(
                42, "delta-v", "exporter-1", "Default", "exporter-1-label");

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, exporter, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasExporterNode()).isTrue();
        assertThat(doc.getExporterNode().getNodeId()).isEqualTo(42);
        assertThat(doc.getExporterNode().getForeignSource()).isEqualTo("delta-v");
        assertThat(doc.getExporterNode().getForeignId()).isEqualTo("exporter-1");
    }

    @Test
    void omitsNodeInfoMessagesWhenLookupReturnedNull() {
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasExporterNode()).isFalse();
        assertThat(doc.hasSrcNode()).isFalse();
        assertThat(doc.hasDestNode()).isFalse();
    }

    @Test
    void populatesApplicationAndLocalityFromEnrichmentParameters() {
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, "HTTPS", "PRIVATE", "PUBLIC", "PUBLIC", null, null, 0L);

        assertThat(doc.getApplication()).isEqualTo("HTTPS");
        assertThat(doc.getSrcLocality()).isEqualTo(FlowDocumentProtos.Locality.PRIVATE);
        assertThat(doc.getDstLocality()).isEqualTo(FlowDocumentProtos.Locality.PUBLIC);
        assertThat(doc.getFlowLocality()).isEqualTo(FlowDocumentProtos.Locality.PUBLIC);
    }

    @Test
    void mapsUInt32ValueWrappersNullSemantics() {
        when(flow.getSrcPort()).thenReturn(null);
        when(flow.getDstPort()).thenReturn(null);
        when(flow.getProtocol()).thenReturn(null);
        when(flow.getDscp()).thenReturn(null);
        when(flow.getEcn()).thenReturn(null);

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasSrcPort()).isFalse();
        assertThat(doc.hasDstPort()).isFalse();
        assertThat(doc.hasProtocol()).isFalse();
        assertThat(doc.hasDscp()).isFalse();
        assertThat(doc.hasEcn()).isFalse();
    }

    @Test
    void mapsUInt32ValueWrappersZeroIsPopulated() {
        when(flow.getSrcPort()).thenReturn(0);
        when(flow.getDstPort()).thenReturn(0);
        when(flow.getProtocol()).thenReturn(0);
        when(flow.getDscp()).thenReturn(0);
        when(flow.getEcn()).thenReturn(0);

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasSrcPort()).isTrue();
        assertThat(doc.getSrcPort().getValue()).isEqualTo(0);
        assertThat(doc.hasDstPort()).isTrue();
        assertThat(doc.hasProtocol()).isTrue();
        assertThat(doc.hasDscp()).isTrue();
        assertThat(doc.hasEcn()).isTrue();
    }

    @Test
    void mapsDirectionEnum() {
        when(flow.getDirection()).thenReturn(Direction.INGRESS);
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getDirection()).isEqualTo(FlowDocumentProtos.Direction.INGRESS);

        when(flow.getDirection()).thenReturn(Direction.EGRESS);
        doc = mapper.map(flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getDirection()).isEqualTo(FlowDocumentProtos.Direction.EGRESS);

        when(flow.getDirection()).thenReturn(Direction.UNKNOWN);
        doc = mapper.map(flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getDirection()).isEqualTo(FlowDocumentProtos.Direction.DIRECTION_UNKNOWN);
    }

    @Test
    void mapsNetflowVersionEnum() {
        when(flow.getNetflowVersion()).thenReturn(NetflowVersion.V9);
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getNetflowVersion()).isEqualTo(FlowDocumentProtos.NetflowVersion.V9);

        when(flow.getNetflowVersion()).thenReturn(NetflowVersion.V5);
        doc = mapper.map(flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getNetflowVersion()).isEqualTo(FlowDocumentProtos.NetflowVersion.V5);

        when(flow.getNetflowVersion()).thenReturn(NetflowVersion.IPFIX);
        doc = mapper.map(flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getNetflowVersion()).isEqualTo(FlowDocumentProtos.NetflowVersion.IPFIX);

        when(flow.getNetflowVersion()).thenReturn(NetflowVersion.SFLOW);
        doc = mapper.map(flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getNetflowVersion()).isEqualTo(FlowDocumentProtos.NetflowVersion.SFLOW);
    }

    @Test
    void mapsSamplingAlgorithmEnum() {
        when(flow.getSamplingAlgorithm()).thenReturn(SamplingAlgorithm.SystematicCountBasedSampling);
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getSamplingAlgorithm())
                .isEqualTo(FlowDocumentProtos.SamplingAlgorithm.SYSTEMATIC_COUNT_BASED_SAMPLING);

        when(flow.getSamplingAlgorithm()).thenReturn(SamplingAlgorithm.Unassigned);
        doc = mapper.map(flow, null, null, null, null, null, null, null, null, null, 0L);
        assertThat(doc.getSamplingAlgorithm())
                .isEqualTo(FlowDocumentProtos.SamplingAlgorithm.SAMPLING_ALGORITHM_UNKNOWN);
    }

    @Test
    void mapsSrcAndDstNodeInfoWhenAllProvided() {
        JdbcNodeInfoLookup.NodeInfo exporter = new JdbcNodeInfoLookup.NodeInfo(
                1, "fs", "exporter", "Default", "exporter-label");
        JdbcNodeInfoLookup.NodeInfo src = new JdbcNodeInfoLookup.NodeInfo(
                2, "fs", "src", "Default", "src-label");
        JdbcNodeInfoLookup.NodeInfo dst = new JdbcNodeInfoLookup.NodeInfo(
                3, "fs", "dst", "Default", "dst-label");

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, exporter, src, dst, null, null, null, null, null, null, 0L);

        assertThat(doc.hasExporterNode()).isTrue();
        assertThat(doc.getExporterNode().getNodeId()).isEqualTo(1);
        assertThat(doc.hasSrcNode()).isTrue();
        assertThat(doc.getSrcNode().getNodeId()).isEqualTo(2);
        assertThat(doc.hasDestNode()).isTrue();
        assertThat(doc.getDestNode().getNodeId()).isEqualTo(3);
    }

    @Test
    void leavesApplicationAsUnknownWhenNullProvided() {
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.getApplication()).isEqualTo("unknown");
        assertThat(doc.getSrcLocality()).isEqualTo(FlowDocumentProtos.Locality.LOCALITY_UNKNOWN);
        assertThat(doc.getDstLocality()).isEqualTo(FlowDocumentProtos.Locality.LOCALITY_UNKNOWN);
        assertThat(doc.getFlowLocality()).isEqualTo(FlowDocumentProtos.Locality.LOCALITY_UNKNOWN);
    }

    @Test
    void populatesHostLocationAndClockCorrectionWhenProvided() {
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null,
                "unknown", "UNKNOWN", "UNKNOWN", "UNKNOWN",
                "router-1.example.com", "MainOffice", 123L);

        assertThat(doc.getHost()).isEqualTo("router-1.example.com");
        assertThat(doc.getLocation()).isEqualTo("MainOffice");
        assertThat(doc.getClockCorrection()).isEqualTo(123L);
    }

    @Test
    void leavesHostAndLocationEmptyWhenNullProvided() {
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null,
                "unknown", "UNKNOWN", "UNKNOWN", "UNKNOWN",
                null, null, 0L);

        assertThat(doc.getHost()).isEmpty();
        assertThat(doc.getLocation()).isEmpty();
        assertThat(doc.getClockCorrection()).isEqualTo(0L);
    }

    @Test
    void omitsSamplingIntervalWhenFlowGetterReturnsNull() {
        when(flow.getSamplingInterval()).thenReturn(null);

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasSamplingInterval()).isFalse();
    }

    @Test
    void populatesSamplingIntervalWhenFlowGetterReturnsZero() {
        when(flow.getSamplingInterval()).thenReturn(0.0);

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null, null, null, null, null, null, null, 0L);

        assertThat(doc.hasSamplingInterval()).isTrue();
        assertThat(doc.getSamplingInterval().getValue()).isEqualTo(0.0);
    }

    @Test
    void setsExporterNodeLabelAndInterfaceNames() {
        JdbcNodeInfoLookup.NodeInfo exporter =
                new JdbcNodeInfoLookup.NodeInfo(7, "nl6", "dev-7", "nl6-lab", "cisco-7");

        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, exporter, null, null,
                "HTTPS", "PRIVATE", "PUBLIC", "PRIVATE",
                "10.0.0.7", "nl6-lab", 0L,
                "Gi0/1", "Gi0/2");

        assertThat(doc.getExporterNode().getNodeLabel()).isEqualTo("cisco-7");
        assertThat(doc.getInputIfName()).isEqualTo("Gi0/1");
        assertThat(doc.getOutputIfName()).isEqualTo("Gi0/2");
    }

    @Test
    void leavesInterfaceNamesEmptyWhenNull() {
        FlowDocumentProtos.FlowDocument doc = mapper.map(
                flow, null, null, null,
                "HTTPS", "PRIVATE", "PUBLIC", "PRIVATE",
                "10.0.0.7", "nl6-lab", 0L,
                null, null);

        assertThat(doc.getInputIfName()).isEmpty();
        assertThat(doc.getOutputIfName()).isEmpty();
    }
}
