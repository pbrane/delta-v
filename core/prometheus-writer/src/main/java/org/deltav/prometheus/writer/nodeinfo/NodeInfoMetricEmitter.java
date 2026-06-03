/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodeinfo;

import org.deltav.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.translate.InstanceLabelResolver;
import org.deltav.prometheus.writer.translate.PromSample;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Sweeps the {@link NodeContextCache} on a schedule and emits Prometheus
 * info-metrics — gauges with constant value {@code 1.0} — that carry node and
 * SNMP-interface attributes as labels. Dashboards join data metrics to these
 * with PromQL {@code group_left} (spec §2.1).
 *
 * <p>{@code deltav_node_info}: one sample per node, carrying the durable
 * {@code node} identity plus node_label / foreign_source / foreign_id /
 * categories. {@code deltav_snmp_interface_info}: one sample per SNMP
 * interface, carrying ifName / ifDescr / ifAlias / ifSpeed / ifType.</p>
 *
 * <p>Re-emitted every {@code emit-interval} so Prometheus/VM does not stale
 * the series. The sink is a {@link Consumer} of {@link PromSample} so the
 * class is unit-testable without a remote-write endpoint.</p>
 */
public class NodeInfoMetricEmitter {

    private static final Logger LOG = LoggerFactory.getLogger(NodeInfoMetricEmitter.class);

    private final NodeContextCache cache;
    private final Consumer<PromSample> sink;

    public NodeInfoMetricEmitter(NodeContextCache cache, Consumer<PromSample> sink) {
        this.cache = cache;
        this.sink = sink;
    }

    /** Emits an info-metric for every node and SNMP interface in the cache. */
    public void sweep() {
        if (!cache.isReady()) {
            LOG.debug("NodeContextCache not yet ready; skipping info-metric sweep");
            return;
        }
        long now = System.currentTimeMillis();
        int nodes = 0;
        int interfaces = 0;
        for (NodeContext nc : cache.snapshot()) {
            String node = InstanceLabelResolver.nodeIdentity(nc.getNodeId(), Optional.of(nc));
            sink.accept(new PromSample("deltav_node_info", nodeLabels(node, nc), 1.0, now));
            nodes++;
            for (SnmpInterfaceContext sic : nc.getSnmpInterfaceMetadataMap().values()) {
                sink.accept(new PromSample(
                        "deltav_snmp_interface_info", interfaceLabels(node, sic), 1.0, now));
                interfaces++;
            }
        }
        LOG.debug("Emitted info-metrics: {} nodes, {} SNMP interfaces", nodes, interfaces);
    }

    private static Map<String, String> nodeLabels(String node, NodeContext nc) {
        Map<String, String> l = new LinkedHashMap<>();
        l.put("node", node);
        l.put("node_id", Integer.toString(nc.getNodeId()));
        l.put("node_label", nc.getNodeLabel());
        l.put("foreign_source", nc.getForeignSource());
        l.put("foreign_id", nc.getForeignId());
        List<String> cats = new ArrayList<>(nc.getCategoriesList());
        cats.sort(String::compareTo);
        l.put("categories", String.join(",", cats));
        l.put("location", nc.getLocation());
        return l;
    }

    private static Map<String, String> interfaceLabels(String node, SnmpInterfaceContext sic) {
        Map<String, String> l = new LinkedHashMap<>();
        l.put("node", node);
        l.put("if_index", Integer.toString(sic.getIfIndex()));
        l.put("if_name", sic.getIfName());
        l.put("if_descr", sic.getIfDescr());
        l.put("if_alias", sic.getIfAlias());
        l.put("if_speed", Long.toString(sic.getIfSpeed()));
        l.put("if_type", Integer.toString(sic.getIfType()));
        l.put("physical_address", sic.getPhysicalAddress());
        return l;
    }
}
