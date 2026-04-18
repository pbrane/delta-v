/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.translate;

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
        labelBuilder = new LabelBuilder(new NameSanitizer());
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

    private NodeContext nc(String nodeLabel, String foreignSource, List<String> categories,
                           Map<String, String> metadata) {
        NodeContext.Builder b = NodeContext.newBuilder()
                .setNodeLabel(nodeLabel)
                .setForeignSource(foreignSource);
        b.addAllCategories(categories);
        b.putAllMetadata(metadata);
        return b.build();
    }

    @Test
    void default_labels_always_emitted() {
        TimeseriesBatch b = batch(42, "Default", "snmp-default", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("router-1", "fs-1", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.keySet()).containsExactlyInAnyOrder(
                "node_id", "location", "node_label", "foreign_source", "categories",
                "resource_type", "resource_instance", "collection_package", "producer");
        assertThat(labels).hasSize(9);
        assertThat(labels.get("node_id")).isEqualTo("42");
        assertThat(labels.get("location")).isEqualTo("Default");
        assertThat(labels.get("node_label")).isEqualTo("router-1");
        assertThat(labels.get("foreign_source")).isEqualTo("fs-1");
        assertThat(labels.get("resource_type")).isEqualTo("node");
        assertThat(labels.get("collection_package")).isEqualTo("snmp-default");
        assertThat(labels.get("producer")).isEqualTo("collectd");
    }

    @Test
    void categories_sorted_comma_joined() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", List.of("production", "critical"), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("categories")).isEqualTo("critical,production");
    }

    @Test
    void categories_empty_when_none() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", Collections.emptyList(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("categories")).isEqualTo("");
    }

    @Test
    void resource_instance_empty_for_non_tabular() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of());

        assertThat(labels.get("resource_instance")).isEqualTo("");
    }

    @Test
    void metadata_allowlist_promotes_listed_keys() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", List.of(), Map.of("requisition:region", "us-east-1"));

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of("requisition:region"));

        assertThat(labels).containsEntry("requisition_region", "us-east-1");
    }

    @Test
    void metadata_allowlist_emits_empty_for_missing_key() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", List.of(), Map.of());

        Map<String, String> labels = labelBuilder.build(b, r, Optional.of(n), List.of("requisition:env"));

        assertThat(labels).containsEntry("requisition_env", "");
    }

    @Test
    void metadata_not_in_allowlist_not_emitted() {
        TimeseriesBatch b = batch(1, "Default", "pkg", ProducerType.PRODUCER_COLLECTD);
        Resource r = resource("node", "");
        NodeContext n = nc("n", "fs", List.of(), Map.of("foo:bar", "baz"));

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
}
