/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.metrics;

/** Centralized Micrometer metric names for the alarms-materializer. */
public final class MaterializerMetrics {
    private MaterializerMetrics() {}

    public static final String RECORDS_CONSUMED       = "deltav_alarms_materializer_records_consumed_total";       // tag: outcome
    public static final String UPSERT_LATENCY         = "deltav_alarms_materializer_upsert_latency_seconds";
    public static final String CONSUMER_LAG_RECORDS   = "deltav_alarms_materializer_consumer_lag_records";
    public static final String RETENTION_EVALS        = "deltav_alarms_materializer_retention_evaluations_total";
    public static final String RETENTION_DELETES      = "deltav_alarms_materializer_retention_deletes_total";     // tag: rule_id
    public static final String RETENTION_TOMBSTONES   = "deltav_alarms_materializer_retention_tombstones_published_total";
    public static final String DB_ERRORS              = "deltav_alarms_materializer_db_errors_total";
    public static final String BOOTSTRAP_COMPLETE     = "deltav_alarms_materializer_bootstrap_complete";

    // Metric tag keys
    public static final String TAG_OUTCOME = "outcome";   // values: upsert | delete | skip
    public static final String TAG_RULE_ID = "rule_id";
}
