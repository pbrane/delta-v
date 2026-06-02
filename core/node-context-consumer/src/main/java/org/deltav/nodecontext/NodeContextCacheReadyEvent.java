/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import org.springframework.context.ApplicationEvent;

/** Published once when {@link NodeContextKafkaBootstrap} finishes initial bootstrap. */
public class NodeContextCacheReadyEvent extends ApplicationEvent {
    private final long bootstrapDurationMs;
    private final int cacheSize;

    public NodeContextCacheReadyEvent(Object source, long bootstrapDurationMs, int cacheSize) {
        super(source);
        this.bootstrapDurationMs = bootstrapDurationMs;
        this.cacheSize = cacheSize;
    }

    public long getBootstrapDurationMs() { return bootstrapDurationMs; }
    public int getCacheSize() { return cacheSize; }
}
