/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.metrics.PrometheusWriterMetrics;
import org.deltav.timeseries.proto.Attribute;
import org.deltav.timeseries.proto.AttributeGroup;
import org.deltav.timeseries.proto.AttributeType;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pure function {@code (TimeseriesBatch, Optional<NodeContext>) -> List<PromSample>}.
 *
 * <p>Filters STRING and UNSPECIFIED attributes (incrementing drop counters).
 * Drops the entire batch and increments the enrichment-missing counter when
 * the {@link NodeContext} is absent. Sample timestamp is the batch collection
 * time, never {@code System.currentTimeMillis()}.</p>
 */
@Component
public class TimeseriesToPromTranslator {
    private final NameSanitizer sanitizer;
    private final LabelBuilder labelBuilder;
    private final PrometheusWriterProperties props;
    private final Counter droppedString;
    private final Counter droppedUnspecified;
    private final Counter enrichmentMissing;

    public TimeseriesToPromTranslator(NameSanitizer sanitizer, LabelBuilder labelBuilder,
                                      PrometheusWriterProperties props, MeterRegistry reg) {
        this.sanitizer = sanitizer;
        this.labelBuilder = labelBuilder;
        this.props = props;
        this.droppedString = reg.counter(PrometheusWriterMetrics.SAMPLES_DROPPED,
                "reason", "string_attribute");
        this.droppedUnspecified = reg.counter(PrometheusWriterMetrics.SAMPLES_DROPPED,
                "reason", "type_unspecified");
        this.enrichmentMissing = reg.counter(PrometheusWriterMetrics.ENRICHMENT_MISSING,
                "reason", "never_seen");
    }

    public List<PromSample> translate(TimeseriesBatch batch, Optional<NodeContext> nc) {
        if (nc.isEmpty()) {
            enrichmentMissing.increment();
            return List.of();
        }
        List<PromSample> out = new ArrayList<>();
        List<String> allowlist = props.labels().fromMetadata();
        for (Resource r : batch.getResourcesList()) {
            Map<String, String> labels = labelBuilder.build(batch, r, nc, allowlist);
            for (AttributeGroup g : r.getGroupsList()) {
                for (Attribute a : g.getAttributesList()) {
                    if (a.getType() == AttributeType.ATTRIBUTE_TYPE_STRING) {
                        droppedString.increment();
                        continue;
                    }
                    if (a.getType() == AttributeType.ATTRIBUTE_TYPE_UNSPECIFIED) {
                        droppedUnspecified.increment();
                        continue;
                    }
                    String name = buildMetricName(g.getName(), a.getName(), a.getType());
                    out.add(new PromSample(name, labels, a.getNumeric(), batch.getTimestampMs()));
                }
            }
        }
        return out;
    }

    private String buildMetricName(String groupName, String attrName, AttributeType type) {
        String base = "opennms_" + sanitizer.sanitize(groupName) + "_" + sanitizer.sanitize(attrName);
        return type == AttributeType.ATTRIBUTE_TYPE_COUNTER ? base + "_total" : base;
    }
}
