/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.metrics;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusWriterMetricsTest {

    private static final Set<String> EXPECTED_METER_NAMES = new TreeSet<>(Set.of(
            "deltav.prometheus.writer.records.consumed",
            "deltav.prometheus.writer.samples.in",
            "deltav.prometheus.writer.records.parse.errors",
            "deltav.prometheus.writer.enrichment.hit",
            "deltav.prometheus.writer.enrichment.missing",
            "deltav.prometheus.writer.enrichment.lookup.fallback",
            "deltav.prometheus.writer.samples.dropped",
            "deltav.prometheus.writer.batches.sent",
            "deltav.prometheus.writer.samples.sent",
            "deltav.prometheus.writer.batches.failed",
            "deltav.prometheus.writer.batch.size.bytes",
            "deltav.prometheus.writer.batch.sample.count",
            "deltav.prometheus.writer.flush.duration",
            "deltav.prometheus.writer.retry.attempts",
            "deltav.prometheus.writer.dlq.records",
            "deltav.prometheus.writer.circuit.state",
            "deltav.prometheus.writer.consumer.paused",
            "deltav.prometheus.writer.distinct.series",
            "deltav.prometheus.writer.labels.per.sample"
    ));

    @Test
    void declared_constants_match_canonical_set() throws Exception {
        Set<String> declared = new TreeSet<>();
        for (Field f : PrometheusWriterMetrics.class.getDeclaredFields()) {
            if (Modifier.isPublic(f.getModifiers())
                    && Modifier.isStatic(f.getModifiers())
                    && Modifier.isFinal(f.getModifiers())
                    && f.getType() == String.class) {
                declared.add((String) f.get(null));
            }
        }
        assertThat(declared).isEqualTo(EXPECTED_METER_NAMES);
    }
}
