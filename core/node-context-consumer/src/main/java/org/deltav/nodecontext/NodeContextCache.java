/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory cache of {@link NodeContext} with a complete lookup API: by
 * {@code {location}@{node_id}} key, by {@code node_id}, and by normalized IP
 * (from {@code interface_metadata}). Reads are lock-free hot-path gets; all
 * mutation is confined to the Kafka consumer thread in NodeContextKafkaBootstrap.
 *
 * <p>IP-conflict policy: if two nodes claim the same IP, the IP index is
 * last-write-wins (mirrors the old JDBC {@code WHERE ipaddr = ? LIMIT 1}).
 */
public class NodeContextCache {

    private final ConcurrentHashMap<String, NodeContext> byKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, NodeContext> byNodeId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NodeContext> byIp = new ConcurrentHashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public Optional<NodeContext> getByKey(String key) {
        return Optional.ofNullable(byKey.get(key));
    }

    public Optional<NodeContext> getByNodeId(int nodeId) {
        return Optional.ofNullable(byNodeId.get(nodeId));
    }

    public Optional<NodeContext> getByIp(String ip) {
        String norm = IpNormalizer.normalize(ip);
        return norm == null ? Optional.empty() : Optional.ofNullable(byIp.get(norm));
    }

    public Optional<String> ifName(int nodeId, int ifIndex) {
        NodeContext n = byNodeId.get(nodeId);
        if (n == null) return Optional.empty();
        SnmpInterfaceContext sic = n.getSnmpInterfaceMetadataMap().get(ifIndex);
        if (sic == null || sic.getIfName().isEmpty()) return Optional.empty();
        return Optional.of(sic.getIfName());
    }

    /**
     * Apply a record. On deleted=true, evicts from all three indexes (using the
     * prior record to find its IPs). On update, removes the prior version's IPs
     * before indexing the new one.
     */
    public void applyUpdate(String key, NodeContext value) {
        NodeContext prior = byKey.get(key);
        if (prior != null) {
            for (String ip : prior.getInterfaceMetadataMap().keySet()) {
                String norm = IpNormalizer.normalize(ip);
                if (norm != null) byIp.remove(norm, prior);
            }
        }
        if (value.getDeleted()) {
            byKey.remove(key);
            if (prior != null) {
                byNodeId.remove(prior.getNodeId(), prior);
            }
            return;
        }
        byKey.put(key, value);
        byNodeId.put(value.getNodeId(), value);
        for (String ip : value.getInterfaceMetadataMap().keySet()) {
            String norm = IpNormalizer.normalize(ip);
            if (norm != null) byIp.put(norm, value);
        }
    }

    public int size() { return byKey.size(); }
    public boolean isReady() { return ready.get(); }
    public void markReady() { ready.set(true); }
    public Collection<NodeContext> snapshot() { return List.copyOf(byKey.values()); }
}
