/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Observes label tuples emitted by {@link org.deltav.prometheus.writer.translate.LabelBuilder}
 * to give operators an early-warning signal on Prometheus series cardinality.
 *
 * <p>Exposes two meters at {@code /actuator/prometheus}:
 * <ul>
 *   <li>{@code deltav_prometheus_writer_distinct_series} — Gauge backed by a
 *       capped Caffeine cache. Reads {@code estimatedSize()} on scrape. Toggled
 *       by {@code prometheus-writer.metrics.cardinality-tracking.enabled}. A
 *       sustained reading at {@code cap} indicates cardinality has exceeded the
 *       budget — operator should prune {@code labels.from-metadata}.</li>
 *   <li>{@code deltav_prometheus_writer_labels_per_sample} — DistributionSummary.
 *       Always emitted; overhead is one atomic increment.</li>
 * </ul>
 *
 * <p>{@link #record(Map)} MUST NEVER throw — observability is best-effort, sample
 * shipping is not. Internal failures are logged at WARN at most once per minute.
 */
@Component
public class LabelCardinalityTracker {

    private static final Logger LOG = LoggerFactory.getLogger(LabelCardinalityTracker.class);
    private static final long WARN_INTERVAL_NANOS = 60L * 1_000_000_000L;
    private static final Object PRESENT = new Object();

    private final boolean enabled;
    private final Cache<String, Object> distinctLabelsets;     // null when disabled
    private final DistributionSummary labelsPerSample;
    private final AtomicLong lastWarnNanos = new AtomicLong(Long.MIN_VALUE);

    public LabelCardinalityTracker(PrometheusWriterProperties props, MeterRegistry registry) {
        var cfg = props.metrics().cardinalityTracking();
        this.enabled = cfg.enabled();
        if (enabled) {
            this.distinctLabelsets = Caffeine.newBuilder()
                    .maximumSize(cfg.cap())
                    .build();
            Gauge.builder(PrometheusWriterMetrics.DISTINCT_SERIES,
                          distinctLabelsets, c -> (double) c.estimatedSize())
                 .description("Distinct label tuples observed since startup, capped at "
                              + cfg.cap() + ". Sustained reading at cap means cardinality "
                              + "exceeds budget — prune labels.from-metadata.")
                 .register(registry);
        } else {
            this.distinctLabelsets = null;
        }
        this.labelsPerSample = DistributionSummary.builder(PrometheusWriterMetrics.LABELS_PER_SAMPLE)
                .description("Number of labels emitted per Prometheus sample.")
                .register(registry);
    }

    /**
     * Observe one label tuple. Never throws.
     *
     * <p>When {@code enabled=false}, only the labels-per-sample summary is recorded;
     * the cache and gauge are bypassed entirely.</p>
     */
    public void record(Map<String, String> labels) {
        if (!enabled) {
            labelsPerSample.record(labels.size());
            return;
        }
        try {
            recordToCache(canonicalize(labels));
            labelsPerSample.record(labels.size());
        } catch (Throwable t) {
            warnRateLimited(t);
        }
    }

    /** Package-private for test override. */
    void recordToCache(String canonical) {
        distinctLabelsets.put(canonical, PRESENT);
    }

    /** Package-private for test forcing of Caffeine maintenance. */
    void cleanUpForTesting() {
        if (distinctLabelsets != null) {
            distinctLabelsets.cleanUp();
        }
    }

    private static String canonicalize(Map<String, String> labels) {
        // Invariant: label values do not contain '\n'. Current sources (LabelBuilder
        // populating from proto string fields + sanitized metadata keys) honor this.
        // If a future label source can carry newlines in values, escape them here
        // or pick a separator that cannot appear in values.
        return labels.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("\n"));
    }

    private void warnRateLimited(Throwable t) {
        long now = System.nanoTime();
        long last = lastWarnNanos.get();
        if (now - last >= WARN_INTERVAL_NANOS && lastWarnNanos.compareAndSet(last, now)) {
            LOG.warn("LabelCardinalityTracker.record() failed; tracker degraded but writer continues", t);
        }
    }
}
