/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.deltav.prometheus.writer.metrics.LabelCardinalityTracker;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Component
public class LabelBuilder {
    private final NameSanitizer sanitizer;
    private final InstanceLabelResolver instanceResolver;
    private final LabelCardinalityTracker tracker;

    public LabelBuilder(NameSanitizer sanitizer,
                        InstanceLabelResolver instanceResolver,
                        LabelCardinalityTracker tracker) {
        this.sanitizer = sanitizer;
        this.instanceResolver = instanceResolver;
        this.tracker = tracker;
    }

    public Map<String, String> build(TimeseriesBatch batch, Resource resource,
                                     Optional<NodeContext> nc, List<String> metadataAllowlist) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("node_id", Integer.toString(batch.getNodeId()));
        labels.put("instance", instanceResolver.resolve(batch, nc));
        labels.put("location", nullToEmpty(batch.getLocation()));
        labels.put("node_label", nc.map(NodeContext::getNodeLabel).orElse(""));
        labels.put("foreign_source", nc.map(NodeContext::getForeignSource).orElse(""));
        labels.put("foreign_id", nc.map(NodeContext::getForeignId).orElse(""));
        labels.put("categories", nc.map(x -> {
            List<String> sorted = new ArrayList<>(x.getCategoriesList());
            Collections.sort(sorted);
            return String.join(",", sorted);
        }).orElse(""));
        labels.put("resource_type", nullToEmpty(resource.getType()));
        labels.put("resource_instance", nullToEmpty(resource.getInstance()));
        labels.put("collection_package", nullToEmpty(batch.getCollectionPackage()));
        labels.put("producer", producerLabel(batch.getProducer()));

        for (String metaKey : metadataAllowlist) {
            String labelName = sanitizer.sanitize(metaKey);
            String value = nc.map(n -> n.getMetadataMap().getOrDefault(metaKey, "")).orElse("");
            labels.put(labelName, value);
        }

        tracker.record(labels);
        return labels;
    }

    private static String producerLabel(ProducerType p) {
        String name = p.name();
        return name.startsWith("PRODUCER_") ? name.substring("PRODUCER_".length()).toLowerCase(Locale.ROOT) : name.toLowerCase(Locale.ROOT);
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }
}
