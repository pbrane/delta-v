/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodeinfo;

import org.deltav.prometheus.writer.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.translate.PromSample;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NodeInfoMetricEmitterTest {

    @Test
    void emitsNodeInfoAndSnmpInterfaceInfoSamples() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder()
                .setNodeId(1042).setLocation("Default")
                .setNodeLabel("web01.corp").setForeignSource("Servers").setForeignId("web-01")
                .addCategories("Production")
                .putSnmpInterfaceMetadata(3, SnmpInterfaceContext.newBuilder()
                        .setIfIndex(3).setIfName("Gi0/3").setIfDescr("GigabitEthernet0/3")
                        .setIfAlias("uplink").setIfSpeed(1_000_000_000L).setIfType(6)
                        .setPhysicalAddress("00:11:22:33:44:55").build())
                .build();
        cache.put("Default@1042", nc);

        List<PromSample> emitted = new ArrayList<>();
        NodeInfoMetricEmitter emitter = new NodeInfoMetricEmitter(cache, emitted::add);

        emitter.sweep();

        PromSample nodeInfo = emitted.stream()
                .filter(s -> s.name().equals("deltav_node_info")).findFirst().orElseThrow();
        assertThat(nodeInfo.value()).isEqualTo(1.0);
        assertThat(nodeInfo.labels()).containsEntry("node", "Servers:web-01");
        assertThat(nodeInfo.labels()).containsEntry("node_id", "1042");
        assertThat(nodeInfo.labels()).containsEntry("node_label", "web01.corp");
        assertThat(nodeInfo.labels()).containsEntry("foreign_source", "Servers");
        assertThat(nodeInfo.labels()).containsEntry("categories", "Production");

        PromSample ifInfo = emitted.stream()
                .filter(s -> s.name().equals("deltav_snmp_interface_info")).findFirst().orElseThrow();
        assertThat(ifInfo.value()).isEqualTo(1.0);
        assertThat(ifInfo.labels()).containsEntry("node", "Servers:web-01");
        assertThat(ifInfo.labels()).containsEntry("if_index", "3");
        assertThat(ifInfo.labels()).containsEntry("if_name", "Gi0/3");
        assertThat(ifInfo.labels()).containsEntry("if_descr", "GigabitEthernet0/3");
        assertThat(ifInfo.labels()).containsEntry("if_alias", "uplink");
        assertThat(ifInfo.labels()).containsEntry("if_speed", "1000000000");
        assertThat(ifInfo.labels()).containsEntry("if_type", "6");
    }

    @Test
    void emitsNothingForEmptyCache() {
        NodeContextCache cache = new NodeContextCache();
        List<PromSample> emitted = new ArrayList<>();
        new NodeInfoMetricEmitter(cache, emitted::add).sweep();
        assertThat(emitted).isEmpty();
    }
}
