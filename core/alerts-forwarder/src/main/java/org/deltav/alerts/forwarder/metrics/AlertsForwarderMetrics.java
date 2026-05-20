/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.metrics;

/** Centralized Micrometer metric names for the alerts-forwarder. */
public final class AlertsForwarderMetrics {
    private AlertsForwarderMetrics() {}

    public static final String ALARMS_CONSUMED = "deltav_alerts_forwarder_alarms_consumed_total";
    public static final String FORWARDED = "deltav_alerts_forwarder_forwarded_total";       // tag: outcome
    public static final String FILTERED = "deltav_alerts_forwarder_filtered_total";         // tag: reason
    public static final String ENRICHMENT = "deltav_alerts_forwarder_enrichment_total";     // tag: result
    public static final String SINK_ERRORS = "deltav_alerts_forwarder_sink_errors_total";   // tag: sink
    public static final String DLQ_RECORDS = "deltav_alerts_forwarder_dlq_total";
    public static final String NC_CACHE_SIZE = "deltav_alerts_forwarder_node_context_cache_size";
    public static final String NC_CACHE_READY = "deltav_alerts_forwarder_node_context_cache_ready";
    public static final String NC_BOOTSTRAP_DURATION = "deltav_alerts_forwarder_node_context_bootstrap_seconds";
    public static final String ACTIVE_ALERTS = "deltav_alerts_forwarder_active_alerts";
}
