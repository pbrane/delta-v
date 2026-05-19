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
        cache.markReady();

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
        assertThat(ifInfo.labels()).containsEntry("physical_address", "00:11:22:33:44:55");
    }

    @Test
    void emitsNothingForEmptyCache() {
        NodeContextCache cache = new NodeContextCache();
        List<PromSample> emitted = new ArrayList<>();
        new NodeInfoMetricEmitter(cache, emitted::add).sweep();
        assertThat(emitted).isEmpty();
    }

    @Test
    void emitsNothingWhenCacheNotReady() {
        NodeContextCache cache = new NodeContextCache();
        // populated but markReady() NOT called — the production "bootstrap replay
        // still draining" window — the guard must short-circuit.
        cache.put("Default@1", NodeContext.newBuilder().setNodeId(1).setLocation("Default").build());

        List<PromSample> emitted = new ArrayList<>();
        new NodeInfoMetricEmitter(cache, emitted::add).sweep();

        assertThat(emitted).isEmpty();
    }

    @Test
    void emitsOneSnmpInterfaceInfoPerInterface() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder()
                .setNodeId(2001).setLocation("Core")
                .setNodeLabel("sw01.corp").setForeignSource("Switches").setForeignId("sw-01")
                .putSnmpInterfaceMetadata(3, SnmpInterfaceContext.newBuilder()
                        .setIfIndex(3).setIfName("Gi0/3").setIfDescr("GigabitEthernet0/3")
                        .setIfAlias("uplink-a").setIfSpeed(1_000_000_000L).setIfType(6)
                        .setPhysicalAddress("AA:BB:CC:DD:EE:03").build())
                .putSnmpInterfaceMetadata(4, SnmpInterfaceContext.newBuilder()
                        .setIfIndex(4).setIfName("Gi0/4").setIfDescr("GigabitEthernet0/4")
                        .setIfAlias("uplink-b").setIfSpeed(1_000_000_000L).setIfType(6)
                        .setPhysicalAddress("AA:BB:CC:DD:EE:04").build())
                .build();
        cache.put("Core@2001", nc);
        cache.markReady();

        List<PromSample> emitted = new ArrayList<>();
        new NodeInfoMetricEmitter(cache, emitted::add).sweep();

        long nodeInfoCount = emitted.stream()
                .filter(s -> s.name().equals("deltav_node_info")).count();
        long ifInfoCount = emitted.stream()
                .filter(s -> s.name().equals("deltav_snmp_interface_info")).count();

        assertThat(nodeInfoCount).isEqualTo(1);
        assertThat(ifInfoCount).isEqualTo(2);

        List<String> ifNames = emitted.stream()
                .filter(s -> s.name().equals("deltav_snmp_interface_info"))
                .map(s -> s.labels().get("if_name"))
                .sorted()
                .toList();
        assertThat(ifNames).containsExactly("Gi0/3", "Gi0/4");
    }
}
