/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.CardinalityTracking;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Labels;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties.Metrics;
import org.deltav.prometheus.writer.metrics.LabelCardinalityTracker;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LabelBuilderTest {

    private LabelBuilder labelBuilder;

    @BeforeEach
    void setUp() {
        // Use a real (no-op) LabelCardinalityTracker disabled-mode so test focus stays on LabelBuilder behavior.
        PrometheusWriterProperties props = new PrometheusWriterProperties(null, null, null, null,
                new Labels(InstanceSource.NODE_LABEL, List.of()),
                new Metrics(new CardinalityTracking(false, 100)),
                null);
        InstanceLabelResolver resolver = new InstanceLabelResolver(props);
        LabelCardinalityTracker tracker = new LabelCardinalityTracker(props, new SimpleMeterRegistry());
        labelBuilder = new LabelBuilder(new NameSanitizer(), resolver, tracker);
    }

    private TimeseriesBatch batch(int nodeId, String location, String collectionPackage, ProducerType producer) {
        return TimeseriesBatch.newBuilder()
                .setNodeId(nodeId)
                .setLocation(location)
                .setCollectionPackage(collectionPackage)
                .setProducer(producer)
                .build();
    }

    private Resource resource(String type, String instance) {
        return Resource.newBuilder()
                .setType(type)
                .setInstance(instance)
                .build();
    }

    private NodeContext nc(String nodeLabel, String foreignSource, String foreignId,
                           List<String> categories, Map<String, String> metadata) {
        NodeContext.Builder b = NodeContext.newBuilder()
                .setNodeLabel(nodeLabel)
                .setForeignSource(foreignSource)
                .setForeignId(foreignId);
        b.addAllCategories(categories);
        b.putAllMetadata(metadata);
        return b.build();
    }

    @Test
    void default_labels_always_emitted() {
        TimeseriesBatch b = batch(42, "Default", "snmp-default", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("router-1", "fs-1", "fi-1", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.keySet()).containsExactlyInAnyOrder(
                "node_id", "node", "instance", "location", "node_label", "foreign_source", "foreign_id",
                "categories", "resource_type", "resource_instance", "collection_package", "producer");
        assertThat(labels).hasSize(12);
        assertThat(labels.get("node_id")).isEqualTo("42");
        assertThat(labels.get("instance")).isEqualTo("router-1");
        assertThat(labels.get("location")).isEqualTo("Default");
        assertThat(labels.get("node_label")).isEqualTo("router-1");
        assertThat(labels.get("foreign_source")).isEqualTo("fs-1");
        assertThat(labels.get("foreign_id")).isEqualTo("fi-1");
        assertThat(labels.get("resource_type")).isEqualTo("node");
        assertThat(labels.get("collection_package")).isEqualTo("snmp-default");
        assertThat(labels.get("producer")).isEqualTo("collectd");
    }

    @Test
    void categories_sorted_comma_joined() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of("production", "critical"), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("categories")).isEqualTo("critical,production");
    }

    @Test
    void categories_empty_when_none() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", Collections.emptyList(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("categories")).isEqualTo("");
    }

    @Test
    void resource_instance_empty_for_non_tabular() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("resource_instance")).isEqualTo("");
    }

    @Test
    void metadata_allowlist_promotes_listed_keys() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of("requisition:region", "us-east-1"));

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of("requisition:region"));

        assertThat(labels).containsEntry("requisition_region", "us-east-1");
    }

    @Test
    void metadata_allowlist_emits_empty_for_missing_key() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of("requisition:env"));

        assertThat(labels).containsEntry("requisition_env", "");
    }

    @Test
    void metadata_not_in_allowlist_not_emitted() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", "fi", List.of(), Map.of("foo:bar", "baz"));

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels).doesNotContainKey("foo_bar");
        assertThat(labels).doesNotContainKey("foo:bar");
    }

    @Test
    void producer_enum_name_stringified() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");

        Map<String, String> labels = labelBuilder.build(b, r, Optional.empty(), List.of());

        assertThat(labels.get("producer")).isEqualTo("collectd");
    }

    @Test
    void instance_label_default_uses_node_label() {
        TimeseriesBatch b = batch(99, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("router-99.prod.example.com", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("instance")).isEqualTo("router-99.prod.example.com");
    }

    @Test
    void instance_label_falls_back_when_node_label_empty() {
        TimeseriesBatch b = batch(99, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("", "fs", "fi", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("instance")).isEqualTo("node:99");
    }

    @Test
    void foreign_id_always_emitted() {
        TimeseriesBatch b = batch(7, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("router-7", "provision-prod", "server-07", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels).containsEntry("foreign_id", "server-07");
    }

    @Test
    void foreign_id_empty_when_node_context_absent() {
        TimeseriesBatch b = batch(7, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");

        Map<String, String> labels = labelBuilder.build(b, r, Optional.empty(), List.of());

        assertThat(labels).containsEntry("foreign_id", "");
    }

    @Test
    void node_label_is_durable_composite_identity() {
        TimeseriesBatch b = batch(1042, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("web01.corp", "Servers", "web-01", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels).containsEntry("node", "Servers:web-01");
    }

    @Test
    void node_label_falls_back_to_delta_v_prefix_for_discovery_node() {
        TimeseriesBatch b = batch(1042, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("scanned-host", "", "", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels).containsEntry("node", "delta-v:1042");
    }
}
