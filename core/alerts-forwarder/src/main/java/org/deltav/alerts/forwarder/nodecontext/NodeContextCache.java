/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.nodecontext;

import org.deltav.timeseries.proto.NodeContext;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory cache of {@link NodeContext} records keyed {@code {location}@{node_id}}.
 * Hot-path {@link #get(String)} is a plain {@link ConcurrentHashMap#get(Object)} —
 * no locking, no network hop. State mutation is confined to the Kafka consumer
 * thread owned by {@link NodeContextKafkaBootstrap}.
 */
@Component
public class NodeContextCache {

    private final ConcurrentHashMap<String, NodeContext> map = new ConcurrentHashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public Optional<NodeContext> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    public void put(String key, NodeContext value) {
        map.put(key, value);
    }

    public void remove(String key) {
        map.remove(key);
    }

    /**
     * Apply a record from the {@code deltav-node-context} topic. If {@code deleted=true},
     * treats as tombstone and removes the key. Otherwise stores the value.
     */
    public void applyUpdate(String key, NodeContext value) {
        if (value.getDeleted()) {
            remove(key);
        } else {
            put(key, value);
        }
    }

    public int size() {
        return map.size();
    }

    public boolean isReady() {
        return ready.get();
    }

    public void markReady() {
        ready.set(true);
    }

    /**
     * An immutable snapshot of every cached entry. Used by the info-metric
     * emitter to sweep all nodes. O(n) copy — called once per emit interval,
     * not on the hot path.
     */
    public Collection<NodeContext> snapshot() {
        return List.copyOf(map.values());
    }

    /**
     * Finds a NodeContext by node_id alone, ignoring location. Used as a
     * fallback when the primary {location}@{node_id} key lookup misses —
     * specifically for the Phase 0 limitation where Delta-V Collectd
     * publishes TimeseriesBatch records with location="" while NodeContext
     * records carry real locations from provisiond. See memory
     * project_kafka_timeseries_producer_next_session for context.
     *
     * <p>O(n) stream scan over the cache. Acceptable because the cache is
     * bounded (~10K nodes max) and this fallback should be rare once
     * Collectd populates location correctly. Returns the first match;
     * deterministic order is not guaranteed across HashMap implementations.
     */
    public Optional<NodeContext> findByNodeId(int nodeId) {
        return map.values().stream()
                .filter(nc -> nc.getNodeId() == nodeId)
                .findFirst();
    }
}
