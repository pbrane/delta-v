/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.metrics.LabelCardinalityTracker;
import org.deltav.timeseries.proto.Attribute;
import org.deltav.timeseries.proto.AttributeGroup;
import org.deltav.timeseries.proto.AttributeType;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TimeseriesToPromTranslatorTest {

    private TimeseriesToPromTranslator translator;
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        NameSanitizer sanitizer = new NameSanitizer();
        PrometheusWriterProperties props = new PrometheusWriterProperties(null, null, null, null,
                new PrometheusWriterProperties.Labels(InstanceSource.NODE_LABEL, List.of()),
                new PrometheusWriterProperties.Metrics(
                        new PrometheusWriterProperties.CardinalityTracking(false, 100)),
                null);
        InstanceLabelResolver resolver = new InstanceLabelResolver(props);
        LabelCardinalityTracker tracker = new LabelCardinalityTracker(props, new SimpleMeterRegistry());
        LabelBuilder labelBuilder = new LabelBuilder(sanitizer, resolver, tracker);
        meterRegistry = new SimpleMeterRegistry();
        translator = new TimeseriesToPromTranslator(sanitizer, labelBuilder, props, meterRegistry);
    }

    private TimeseriesBatch batch(int nodeId, long timestampMs, AttributeGroup... groups) {
        Resource.Builder resource = Resource.newBuilder()
                .setType("node")
                .setInstance("");
        for (AttributeGroup g : groups) {
            resource.addGroups(g);
        }
        return TimeseriesBatch.newBuilder()
                .setNodeId(nodeId)
                .setTimestampMs(timestampMs)
                .setLocation("Default")
                .setCollectionPackage("snmp-default")
                .setProducer(ProducerType.PRODUCER_COLLECTD)
                .addResources(resource.build())
                .build();
    }

    private AttributeGroup group(String name, Attribute... attrs) {
        AttributeGroup.Builder b = AttributeGroup.newBuilder().setName(name);
        for (Attribute a : attrs) {
            b.addAttributes(a);
        }
        return b.build();
    }

    private Attribute attr(String name, AttributeType type, double value) {
        return Attribute.newBuilder()
                .setName(name)
                .setType(type)
                .setNumeric(value)
                .build();
    }

    private NodeContext nc(int nodeId, String label, String fs) {
        return NodeContext.newBuilder()
                .setNodeId(nodeId)
                .setNodeLabel(label)
                .setForeignSource(fs)
                .build();
    }

    @Test
    void counter_attribute_gets_total_suffix() {
        TimeseriesBatch b = batch(1, 1_700_000_000_000L,
                group("mib2-interface-errors", attr("ifInDiscards", AttributeType.ATTRIBUTE_TYPE_COUNTER, 5.0)));
        NodeContext n = nc(1, "router-1", "fs-1");

        List<PromSample> samples = translator.translate(b, Optional.of(n));

        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).name()).isEqualTo("opennms_mib2_interface_errors_ifindiscards_total");
        assertThat(samples.get(0).value()).isEqualTo(5.0);
    }

    @Test
    void gauge_attribute_no_total_suffix() {
        TimeseriesBatch b = batch(1, 1_700_000_000_000L,
                group("mib2-X-interfaces", attr("ifHighSpeed", AttributeType.ATTRIBUTE_TYPE_GAUGE, 1000.0)));
        NodeContext n = nc(1, "router-1", "fs-1");

        List<PromSample> samples = translator.translate(b, Optional.of(n));

        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).name()).isEqualTo("opennms_mib2_x_interfaces_ifhighspeed");
        assertThat(samples.get(0).name()).doesNotEndWith("_total");
    }

    @Test
    void string_attribute_dropped() {
        TimeseriesBatch b = batch(1, 1_700_000_000_000L,
                group("sysinfo", attr("sysName", AttributeType.ATTRIBUTE_TYPE_STRING, 0.0)));
        NodeContext n = nc(1, "router-1", "fs-1");

        List<PromSample> samples = translator.translate(b, Optional.of(n));

        assertThat(samples).isEmpty();
        assertThat(meterRegistry.find("deltav.prometheus.writer.samples.dropped")
                .tag("reason", "string_attribute").counter().count()).isEqualTo(1.0);
    }

    @Test
    void unspecified_attribute_dropped() {
        TimeseriesBatch b = batch(1, 1_700_000_000_000L,
                group("legacy", attr("oldMetric", AttributeType.ATTRIBUTE_TYPE_UNSPECIFIED, 1.0)));
        NodeContext n = nc(1, "router-1", "fs-1");

        List<PromSample> samples = translator.translate(b, Optional.of(n));

        assertThat(samples).isEmpty();
        assertThat(meterRegistry.find("deltav.prometheus.writer.samples.dropped")
                .tag("reason", "type_unspecified").counter().count()).isEqualTo(1.0);
    }

    @Test
    void missing_node_context_drops_all_samples() {
        TimeseriesBatch b = batch(99, 1_700_000_000_000L,
                group("g",
                        attr("a1", AttributeType.ATTRIBUTE_TYPE_COUNTER, 1.0),
                        attr("a2", AttributeType.ATTRIBUTE_TYPE_GAUGE, 2.0),
                        attr("a3", AttributeType.ATTRIBUTE_TYPE_COUNTER, 3.0)));

        List<PromSample> samples = translator.translate(b, Optional.empty());

        assertThat(samples).isEmpty();
        assertThat(meterRegistry.find("deltav.prometheus.writer.enrichment.missing")
                .tag("reason", "never_seen").counter().count()).isEqualTo(1.0);
    }

    @Test
    void timestamp_from_batch_collection_time() {
        long collectionTime = 1_700_000_000_000L;
        TimeseriesBatch b = batch(1, collectionTime,
                group("g", attr("a", AttributeType.ATTRIBUTE_TYPE_GAUGE, 1.0)));
        NodeContext n = nc(1, "router-1", "fs-1");

        List<PromSample> samples = translator.translate(b, Optional.of(n));

        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).timestampMs()).isEqualTo(collectionTime);
        assertThat(samples.get(0).timestampMs()).isNotEqualTo(System.currentTimeMillis());
    }

    @Test
    void all_labels_populated() {
        TimeseriesBatch b = batch(1, 1_700_000_000_000L,
                group("mib2-interface-errors",
                        attr("ifInDiscards", AttributeType.ATTRIBUTE_TYPE_COUNTER, 5.0)));
        NodeContext n = nc(1, "router-1", "fs-1");

        List<PromSample> samples = translator.translate(b, Optional.of(n));

        assertThat(samples).hasSize(1);
        Map<String, String> labels = samples.get(0).labels();
        assertThat(labels.keySet()).containsExactlyInAnyOrder(
                "node_id", "node", "instance", "location", "node_label", "foreign_source", "foreign_id",
                "categories", "resource_type", "resource_instance", "collection_package", "producer");
        assertThat(labels).hasSize(12);
    }
}
