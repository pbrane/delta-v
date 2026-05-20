/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.enrich;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.nodecontext.NodeContextCache;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.SnmpInterfaceContext;
import org.springframework.stereotype.Component;

/**
 * Joins an {@link AlarmState} to its {@link NodeContext} and produces an
 * {@link EnrichedAlarm}. Lookup key is {@code {location}@{node_id}}; on a miss
 * (Collectd/alarmd location skew, or a node not yet materialized) it falls back
 * to a node-id-only scan, and failing that produces a partial enrichment.
 */
@Component
public class AlarmEnricher {

    private final NodeContextCache cache;

    public AlarmEnricher(NodeContextCache cache) {
        this.cache = cache;
    }

    public EnrichedAlarm enrich(AlarmState alarm) {
        Optional<NodeContext> nc = lookup(alarm);

        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("node", NodeIdentity.of(alarm.getNodeId(), nc));
        labels.put("node_id", Integer.toString(alarm.getNodeId()));
        labels.put("severity", alarm.getSeverity().name());
        labels.put("uei", alarm.getUei());
        labels.put("location", alarm.getLocation());
        labels.put("alarm_id", Integer.toString(alarm.getAlarmId()));
        labels.put("node_label", nc.map(NodeContext::getNodeLabel).orElse(""));
        labels.put("foreign_source", nc.map(NodeContext::getForeignSource).orElse(""));
        labels.put("foreign_id", nc.map(NodeContext::getForeignId).orElse(""));
        labels.put("categories", categories(nc));

        if (!alarm.getServiceName().isEmpty()) {
            labels.put("service_name", alarm.getServiceName());
        }
        if (!alarm.getIpAddress().isEmpty()) {
            labels.put("ip_address", alarm.getIpAddress());
        }
        if (alarm.getIfIndex() > 0) {
            labels.put("if_index", Integer.toString(alarm.getIfIndex()));
            nc.map(n -> n.getSnmpInterfaceMetadataMap().get(alarm.getIfIndex()))
              .ifPresent(sic -> addInterfaceLabels(labels, sic));
        }

        boolean complete = nc.isPresent();
        if (!complete) {
            labels.put("enrichment", "partial");
        }

        Map<String, String> annotations = new LinkedHashMap<>();
        annotations.put("description", alarm.getDescription());
        annotations.put("log_message", alarm.getLogMessage());
        if (!alarm.getAckUser().isEmpty()) {
            annotations.put("ack_user", alarm.getAckUser());
        }

        return new EnrichedAlarm(labels, annotations, complete);
    }

    private Optional<NodeContext> lookup(AlarmState alarm) {
        Optional<NodeContext> nc = cache.get(alarm.getLocation() + "@" + alarm.getNodeId());
        if (nc.isEmpty() && alarm.getNodeId() > 0) {
            nc = cache.findByNodeId(alarm.getNodeId());
        }
        return nc;
    }

    private static void addInterfaceLabels(Map<String, String> labels, SnmpInterfaceContext sic) {
        labels.put("if_name", sic.getIfName());
        labels.put("if_descr", sic.getIfDescr());
        labels.put("if_alias", sic.getIfAlias());
        labels.put("if_speed", Long.toString(sic.getIfSpeed()));
        labels.put("if_type", Integer.toString(sic.getIfType()));
    }

    private static String categories(Optional<NodeContext> nc) {
        List<String> cats = new ArrayList<>(
                nc.map(n -> (List<String>) new ArrayList<>(n.getCategoriesList())).orElse(List.of()));
        cats.sort(String::compareTo);
        return String.join(",", cats);
    }
}
