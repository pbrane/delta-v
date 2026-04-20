/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

/**
 * Source policy for the Prometheus-ecosystem {@code instance} label.
 * Bound from {@code prometheus-writer.labels.instance-source} in
 * {@code application.yml} via Spring Boot configuration binding.
 */
public enum InstanceSource {
    /** Use {@code NodeContext.nodeLabel} (default; matches Grafana convention). */
    NODE_LABEL,
    /** Use {@code "{foreign_source}:{foreign_id}"} (stable across renames). */
    FOREIGN_ID,
    /** Use {@code "node:{node_id}"} (pure stable integer key). */
    NODE_ID
}
