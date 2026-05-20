/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.enrich;

import java.util.Map;

/**
 * An alarm joined to node context. {@code labels} are low-cardinality
 * dimensions safe for a TSDB series (identity, severity, uei, interface
 * attributes); {@code annotations} are unbounded free text (description,
 * log message) — Alertmanager-only, never a label. {@code enrichmentComplete}
 * is false when the node was not found in the cache (partial enrichment).
 */
public record EnrichedAlarm(
        Map<String, String> labels,
        Map<String, String> annotations,
        boolean enrichmentComplete) {
}
