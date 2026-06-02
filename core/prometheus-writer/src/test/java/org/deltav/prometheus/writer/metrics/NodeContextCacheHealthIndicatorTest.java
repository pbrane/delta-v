/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

import org.deltav.nodecontext.NodeContextCache;
import org.deltav.nodecontext.NodeContextCacheHealthIndicator;
import org.deltav.timeseries.proto.NodeContext;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import static org.assertj.core.api.Assertions.assertThat;

class NodeContextCacheHealthIndicatorTest {

    @Test
    void health_down_when_cache_not_ready() {
        NodeContextCache cache = new NodeContextCache();
        // not marked ready, no entries
        Health health = new NodeContextCacheHealthIndicator(cache).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("ready", false);
        assertThat(health.getDetails()).containsEntry("size", 0);
    }

    @Test
    void health_up_when_cache_ready() {
        NodeContextCache cache = new NodeContextCache();
        cache.applyUpdate("Default@1", NodeContext.newBuilder().setNodeId(1).build());
        cache.applyUpdate("Default@2", NodeContext.newBuilder().setNodeId(2).build());
        cache.markReady();

        Health health = new NodeContextCacheHealthIndicator(cache).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("ready", true);
        assertThat(health.getDetails()).containsEntry("size", 2);
    }
}
