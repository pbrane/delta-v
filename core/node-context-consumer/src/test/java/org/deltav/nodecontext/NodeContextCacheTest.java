/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import static org.assertj.core.api.Assertions.assertThat;

import org.deltav.timeseries.proto.InterfaceContext;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.junit.jupiter.api.Test;

class NodeContextCacheTest {

    private static NodeContext node(int id, String location, String label) {
        return NodeContext.newBuilder().setNodeId(id).setLocation(location).setNodeLabel(label).build();
    }
    private static String key(String location, int id) { return location + "@" + id; }

    @Test
    void getByNodeIdAndByKeyResolve() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7");
        c.applyUpdate(key("Default", 7), n);
        assertThat(c.getByKey("Default@7")).contains(n);
        assertThat(c.getByNodeId(7)).contains(n);
    }

    @Test
    void getByIpUsesInterfaceMetadataAndNormalizes() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7").toBuilder()
                .putInterfaceMetadata("10.0.0.1", InterfaceContext.getDefaultInstance()).build();
        c.applyUpdate(key("Default", 7), n);
        assertThat(c.getByIp("10.0.0.1")).contains(n);
        assertThat(c.getByIp("010.000.000.001")).contains(n);
        assertThat(c.getByIp("::ffff:10.0.0.1")).contains(n);
        assertThat(c.getByIp("198.51.100.9")).isEmpty();
    }

    @Test
    void getByIpHandlesCompressedVsExpandedIpv6() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(8, "Default", "v6").toBuilder()
                .putInterfaceMetadata("2001:db8::1", InterfaceContext.getDefaultInstance()).build();
        c.applyUpdate(key("Default", 8), n);
        assertThat(c.getByIp("2001:db8:0:0:0:0:0:1")).contains(n);
        assertThat(c.getByIp("2001:DB8::1")).contains(n);
    }

    @Test
    void ifNameResolvesFromSnmpInterfaceMetadata() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7").toBuilder()
                .putSnmpInterfaceMetadata(3, SnmpInterfaceContext.newBuilder().setIfIndex(3).setIfName("Gi0/3").build()).build();
        c.applyUpdate(key("Default", 7), n);
        assertThat(c.ifName(7, 3)).contains("Gi0/3");
        assertThat(c.ifName(7, 99)).isEmpty();
        assertThat(c.ifName(404, 3)).isEmpty();
    }

    @Test
    void tombstoneEvictsKeyNodeIdAndIps() {
        NodeContextCache c = new NodeContextCache();
        NodeContext n = node(7, "Default", "router-7").toBuilder()
                .putInterfaceMetadata("10.0.0.1", InterfaceContext.getDefaultInstance()).build();
        c.applyUpdate(key("Default", 7), n);
        assertThat(c.getByIp("10.0.0.1")).isPresent();
        c.applyUpdate(key("Default", 7), NodeContext.newBuilder().setNodeId(7).setLocation("Default").setDeleted(true).build());
        assertThat(c.getByKey("Default@7")).isEmpty();
        assertThat(c.getByNodeId(7)).isEmpty();
        assertThat(c.getByIp("10.0.0.1")).isEmpty();
    }

    @Test
    void ipConflictIsLastWriteWins() {
        NodeContextCache c = new NodeContextCache();
        NodeContext a = node(1, "Default", "a").toBuilder().putInterfaceMetadata("10.0.0.30", InterfaceContext.getDefaultInstance()).build();
        NodeContext b = node(2, "Default", "b").toBuilder().putInterfaceMetadata("10.0.0.30", InterfaceContext.getDefaultInstance()).build();
        c.applyUpdate(key("Default", 1), a);
        c.applyUpdate(key("Default", 2), b);
        assertThat(c.getByIp("10.0.0.30")).contains(b);
        c.applyUpdate(key("Default", 1), NodeContext.newBuilder().setNodeId(1).setLocation("Default").setDeleted(true).build());
        assertThat(c.getByIp("10.0.0.30")).contains(b);
    }

    @Test
    void updateRemovesStaleIpsFromPreviousVersion() {
        NodeContextCache c = new NodeContextCache();
        c.applyUpdate(key("Default", 7), node(7, "Default", "r").toBuilder().putInterfaceMetadata("10.0.0.1", InterfaceContext.getDefaultInstance()).build());
        c.applyUpdate(key("Default", 7), node(7, "Default", "r").toBuilder().putInterfaceMetadata("10.0.0.2", InterfaceContext.getDefaultInstance()).build());
        assertThat(c.getByIp("10.0.0.1")).isEmpty();
        assertThat(c.getByIp("10.0.0.2")).isPresent();
    }
}
