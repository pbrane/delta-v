/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.CardinalityTracking;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LabelCardinalityTrackerTest {

    private MeterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
    }

    private PrometheusWriterProperties props(boolean enabled, int cap) {
        return new PrometheusWriterProperties(null, null, null, null, null,
                new Metrics(new CardinalityTracking(enabled, cap)), null);
    }

    private LabelCardinalityTracker tracker(boolean enabled, int cap) {
        return new LabelCardinalityTracker(props(enabled, cap), registry);
    }

    @Test
    void enabled_recordsDistinctLabelsetsToGauge() {
        LabelCardinalityTracker t = tracker(true, 100);
        t.record(Map.of("a", "1"));
        t.record(Map.of("a", "2"));
        t.record(Map.of("a", "3"));

        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g).isNotNull();
        assertThat(g.value()).isEqualTo(3.0);
    }

    @Test
    void enabled_duplicateLabelsetCountsOnce() {
        LabelCardinalityTracker t = tracker(true, 100);
        t.record(Map.of("a", "1", "b", "x"));
        t.record(Map.of("a", "1", "b", "x"));
        t.record(Map.of("a", "1", "b", "x"));

        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g.value()).isEqualTo(1.0);
    }

    @Test
    void enabled_capExceeded_evictsAndStaysAtCap() {
        LabelCardinalityTracker t = tracker(true, 2);
        t.record(Map.of("a", "1"));
        t.record(Map.of("a", "2"));
        t.record(Map.of("a", "3"));
        t.record(Map.of("a", "4"));
        t.record(Map.of("a", "5"));

        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        // Force Caffeine maintenance to settle the size to <= cap.
        t.cleanUpForTesting();
        assertThat(g.value()).isLessThanOrEqualTo(2.0);
    }

    @Test
    void enabled_recordsLabelCountToDistributionSummary() {
        LabelCardinalityTracker t = tracker(true, 100);
        t.record(Map.of("a", "1", "b", "2", "c", "3"));    // 3 labels
        t.record(Map.of("x", "1"));                         // 1 label

        DistributionSummary s = registry.find("deltav.prometheus.writer.labels.per.sample").summary();
        assertThat(s).isNotNull();
        assertThat(s.count()).isEqualTo(2);
        assertThat(s.totalAmount()).isEqualTo(4.0);
    }

    @Test
    void disabled_recordIsNoOpForGauge_butStillRecordsDistributionSummary() {
        LabelCardinalityTracker t = tracker(false, 100);
        t.record(Map.of("a", "1"));
        t.record(Map.of("a", "2"));

        // Gauge should not be registered when disabled.
        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g).isNull();

        // DistributionSummary IS still registered and recorded.
        DistributionSummary s = registry.find("deltav.prometheus.writer.labels.per.sample").summary();
        assertThat(s).isNotNull();
        assertThat(s.count()).isEqualTo(2);
    }

    @Test
    void recordSurvivesInternalThrow() {
        // Override recordToCache to throw — verifies the catch in record() prevents propagation.
        LabelCardinalityTracker throwing = new LabelCardinalityTracker(props(true, 100), registry) {
            @Override
            void recordToCache(String canonical) {
                throw new RuntimeException("boom");
            }
        };
        throwing.record(Map.of("a", "1"));   // must NOT throw
        throwing.record(Map.of("b", "2"));   // must NOT throw

        // record() puts to cache BEFORE recording to summary — when recordToCache throws,
        // labelsPerSample.record(...) is never reached. Confirm zero count.
        DistributionSummary s = registry.find("deltav.prometheus.writer.labels.per.sample").summary();
        assertThat(s.count()).isEqualTo(0);
    }

    @Test
    void canonicalizationOrderIndependent() {
        LabelCardinalityTracker t = tracker(true, 100);

        Map<String, String> first = new LinkedHashMap<>();
        first.put("a", "1");
        first.put("b", "2");
        first.put("c", "3");

        Map<String, String> sameInDifferentOrder = new LinkedHashMap<>();
        sameInDifferentOrder.put("c", "3");
        sameInDifferentOrder.put("a", "1");
        sameInDifferentOrder.put("b", "2");

        t.record(first);
        t.record(sameInDifferentOrder);

        Gauge g = registry.find("deltav.prometheus.writer.distinct.series").gauge();
        assertThat(g.value()).isEqualTo(1.0);
    }
}
