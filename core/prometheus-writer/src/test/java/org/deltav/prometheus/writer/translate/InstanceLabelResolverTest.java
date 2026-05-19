/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Labels;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InstanceLabelResolverTest {

    private static PrometheusWriterProperties propsWith(InstanceSource source) {
        return new PrometheusWriterProperties(null, null, null, null,
                new Labels(source, List.of()), null, null);
    }

    private static InstanceLabelResolver resolver(InstanceSource source) {
        return new InstanceLabelResolver(propsWith(source));
    }

    private static TimeseriesBatch batch(int nodeId) {
        return TimeseriesBatch.newBuilder().setNodeId(nodeId).setLocation("Default").build();
    }

    private static NodeContext nc(String nodeLabel, String foreignSource, String foreignId) {
        return NodeContext.newBuilder()
                .setNodeLabel(nodeLabel)
                .setForeignSource(foreignSource)
                .setForeignId(foreignId)
                .build();
    }

    @Test
    void nodeLabelSource_returnsNodeLabel() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_LABEL);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "fs", "fi"))))
                .isEqualTo("router-1");
    }

    @Test
    void nodeLabelSource_emptyNodeLabel_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_LABEL);
        assertThat(r.resolve(batch(42), Optional.of(nc("", "fs", "fi"))))
                .isEqualTo("node:42");
    }

    @Test
    void nodeLabelSource_absentNodeContext_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_LABEL);
        assertThat(r.resolve(batch(42), Optional.empty()))
                .isEqualTo("node:42");
    }

    @Test
    void foreignIdSource_bothPresent_concatenates() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "provision-prod", "server-01"))))
                .isEqualTo("provision-prod:server-01");
    }

    @Test
    void foreignIdSource_emptyForeignSource_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "", "server-01"))))
                .isEqualTo("delta-v:42");
    }

    @Test
    void foreignIdSource_emptyForeignId_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "provision-prod", ""))))
                .isEqualTo("delta-v:42");
    }

    @Test
    void foreignIdSource_absentNodeContext_fallsBackToNodeId() {
        InstanceLabelResolver r = resolver(InstanceSource.FOREIGN_ID);
        assertThat(r.resolve(batch(42), Optional.empty()))
                .isEqualTo("delta-v:42");
    }

    @Test
    void nodeIdSource_alwaysReturnsNodeIdForm() {
        InstanceLabelResolver r = resolver(InstanceSource.NODE_ID);
        assertThat(r.resolve(batch(42), Optional.of(nc("router-1", "fs", "fi"))))
                .isEqualTo("node:42");
        assertThat(r.resolve(batch(7), Optional.empty())).isEqualTo("node:7");
    }

    // --- nodeIdentity static helper ---

    @Test
    void nodeIdentity_usesForeignSourceColonForeignId() {
        NodeContext n = NodeContext.newBuilder()
                .setNodeId(1042).setForeignSource("Servers").setForeignId("web-01").build();
        assertThat(InstanceLabelResolver.nodeIdentity(1042, Optional.of(n)))
                .isEqualTo("Servers:web-01");
    }

    @Test
    void nodeIdentity_fallsBackToDeltaVPrefixWhenNoForeignSource() {
        NodeContext n = NodeContext.newBuilder().setNodeId(1042).build();
        assertThat(InstanceLabelResolver.nodeIdentity(1042, Optional.of(n)))
                .isEqualTo("delta-v:1042");
    }

    @Test
    void nodeIdentity_fallsBackWhenNodeContextAbsent() {
        assertThat(InstanceLabelResolver.nodeIdentity(77, Optional.empty()))
                .isEqualTo("delta-v:77");
    }

    @Test
    void nodeIdentity_fallsBackWhenForeignIdEmpty() {
        NodeContext n = NodeContext.newBuilder()
                .setNodeId(5).setForeignSource("Servers").build();   // foreignId empty
        assertThat(InstanceLabelResolver.nodeIdentity(5, Optional.of(n)))
                .isEqualTo("delta-v:5");
    }
}
