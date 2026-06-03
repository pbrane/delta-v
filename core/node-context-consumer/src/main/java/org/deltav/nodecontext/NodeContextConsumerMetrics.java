/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

/** Centralized Micrometer metric names for the node-context-consumer library. */
public final class NodeContextConsumerMetrics {
    private NodeContextConsumerMetrics() {}

    public static final String NC_CACHE_SIZE = "deltav_node_context_cache_size";
    public static final String NC_CACHE_READY = "deltav_node_context_cache_ready";
    public static final String NC_BOOTSTRAP_DURATION = "deltav_node_context_bootstrap_seconds";
}
