/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Resolves the Prometheus-ecosystem {@code instance} label per the configured
 * {@link InstanceSource} policy. Always returns a non-empty string — falls back
 * to {@code "node:{node_id}"} whenever the chosen source produces an empty
 * value, so {@code {instance=""}} never appears on the wire (Grafana templating
 * unhappiness).
 */
@Component
public class InstanceLabelResolver {

    private final InstanceSource source;

    public InstanceLabelResolver(PrometheusWriterProperties props) {
        this.source = props.labels().instanceSource();
    }

    public String resolve(TimeseriesBatch batch, Optional<NodeContext> nc) {
        return switch (source) {
            case NODE_LABEL -> {
                String label = nc.map(NodeContext::getNodeLabel).orElse("");
                yield label.isEmpty() ? fallback(batch) : label;
            }
            case FOREIGN_ID -> {
                String fs = nc.map(NodeContext::getForeignSource).orElse("");
                String fi = nc.map(NodeContext::getForeignId).orElse("");
                yield (fs.isEmpty() || fi.isEmpty()) ? fallback(batch) : fs + ":" + fi;
            }
            case NODE_ID -> fallback(batch);
        };
    }

    private static String fallback(TimeseriesBatch batch) {
        return "node:" + batch.getNodeId();
    }
}
