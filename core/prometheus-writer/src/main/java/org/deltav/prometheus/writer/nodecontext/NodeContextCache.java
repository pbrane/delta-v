/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import org.deltav.timeseries.proto.NodeContext;
import org.springframework.stereotype.Component;

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
}
