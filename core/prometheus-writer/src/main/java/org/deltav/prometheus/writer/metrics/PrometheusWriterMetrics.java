/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

/**
 * Canonical Micrometer meter names for the prometheus-writer. Every meter
 * declared here MUST appear at /actuator/prometheus once the service is fully
 * wired. {@link PrometheusWriterMetricsTest} pins the list.
 */
public final class PrometheusWriterMetrics {
    private PrometheusWriterMetrics() {}

    // ingestion
    public static final String RECORDS_CONSUMED        = "deltav.prometheus.writer.records.consumed";
    public static final String SAMPLES_IN              = "deltav.prometheus.writer.samples.in";
    public static final String RECORDS_PARSE_ERRORS    = "deltav.prometheus.writer.records.parse.errors";

    // enrichment
    public static final String ENRICHMENT_HIT          = "deltav.prometheus.writer.enrichment.hit";
    public static final String ENRICHMENT_MISSING      = "deltav.prometheus.writer.enrichment.missing";
    public static final String ENRICHMENT_LOOKUP_FALLBACK = "deltav.prometheus.writer.enrichment.lookup.fallback";

    // sample dropouts
    public static final String SAMPLES_DROPPED         = "deltav.prometheus.writer.samples.dropped";

    // rw output
    public static final String BATCHES_SENT            = "deltav.prometheus.writer.batches.sent";
    public static final String SAMPLES_SENT            = "deltav.prometheus.writer.samples.sent";
    public static final String BATCHES_FAILED          = "deltav.prometheus.writer.batches.failed";
    public static final String BATCH_SIZE_BYTES        = "deltav.prometheus.writer.batch.size.bytes";
    public static final String BATCH_SAMPLE_COUNT      = "deltav.prometheus.writer.batch.sample.count";
    public static final String FLUSH_DURATION          = "deltav.prometheus.writer.flush.duration";
    public static final String RETRY_ATTEMPTS          = "deltav.prometheus.writer.retry.attempts";

    // dlq
    public static final String DLQ_RECORDS             = "deltav.prometheus.writer.dlq.records";

    // circuit + consumer state
    public static final String CIRCUIT_STATE           = "deltav.prometheus.writer.circuit.state";
    public static final String CONSUMER_PAUSED         = "deltav.prometheus.writer.consumer.paused";

    // cardinality observability
    public static final String DISTINCT_SERIES         = "deltav.prometheus.writer.distinct.series";
    public static final String LABELS_PER_SAMPLE       = "deltav.prometheus.writer.labels.per.sample";
}
