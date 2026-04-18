/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import org.deltav.timeseries.proto.NodeContext;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class NodeContextCacheTest {

    @Test
    void get_returns_empty_when_key_absent() {
        NodeContextCache cache = new NodeContextCache();
        assertThat(cache.get("Default@1")).isEmpty();
    }

    @Test
    void put_stores_value_and_get_retrieves() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder().setNodeId(1).setLocation("Default").setNodeLabel("lab-1").build();
        cache.put("Default@1", nc);
        assertThat(cache.get("Default@1")).contains(nc);
    }

    @Test
    void remove_evicts_key() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder().setNodeId(1).build();
        cache.put("Default@1", nc);
        cache.remove("Default@1");
        assertThat(cache.get("Default@1")).isEmpty();
    }

    @Test
    void tombstone_on_put_delegates_to_remove() {
        NodeContextCache cache = new NodeContextCache();
        cache.put("Default@1", NodeContext.newBuilder().setNodeId(1).build());
        NodeContext tombstone = NodeContext.newBuilder().setNodeId(1).setDeleted(true).build();
        cache.applyUpdate("Default@1", tombstone);
        assertThat(cache.get("Default@1")).isEmpty();
    }

    @Test
    void applyUpdate_live_record_puts() {
        NodeContextCache cache = new NodeContextCache();
        NodeContext nc = NodeContext.newBuilder().setNodeId(1).setDeleted(false).build();
        cache.applyUpdate("Default@1", nc);
        assertThat(cache.get("Default@1")).contains(nc);
    }

    @Test
    void size_reports_current_entries() {
        NodeContextCache cache = new NodeContextCache();
        assertThat(cache.size()).isZero();
        cache.put("Default@1", NodeContext.newBuilder().setNodeId(1).build());
        cache.put("Default@2", NodeContext.newBuilder().setNodeId(2).build());
        assertThat(cache.size()).isEqualTo(2);
        cache.remove("Default@1");
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    void ready_defaults_false_and_markReady_flips() {
        NodeContextCache cache = new NodeContextCache();
        assertThat(cache.isReady()).isFalse();
        cache.markReady();
        assertThat(cache.isReady()).isTrue();
    }
}
