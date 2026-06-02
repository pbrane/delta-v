/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public class NodeContextCacheHealthIndicator implements HealthIndicator {
    private final NodeContextCache cache;

    public NodeContextCacheHealthIndicator(NodeContextCache cache) {
        this.cache = cache;
    }

    @Override
    public Health health() {
        Health.Builder b = cache.isReady() ? Health.up() : Health.down();
        return b.withDetail("ready", cache.isReady())
                .withDetail("size", cache.size())
                .build();
    }
}
