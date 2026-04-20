/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.micrometer.core.instrument.MeterRegistry;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.prometheus.writer.PrometheusWriterApplication;
import org.deltav.timeseries.proto.Attribute;
import org.deltav.timeseries.proto.AttributeGroup;
import org.deltav.timeseries.proto.AttributeType;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.xerial.snappy.Snappy;
import prometheus.prompb.Label;
import prometheus.prompb.TimeSeries;
import prometheus.prompb.WriteRequest;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(classes = PrometheusWriterApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RwRoundTripIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"))
                    .withStartupTimeout(Duration.ofSeconds(120));

    static MockWebServer mockRw;

    @BeforeAll
    static void startMock() throws Exception {
        mockRw = new MockWebServer();
        mockRw.start();
    }

    @AfterAll
    static void stopMock() throws Exception {
        if (mockRw != null) mockRw.shutdown();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry reg) {
        // Topics MUST exist before Spring boots: NodeContextKafkaBootstrap publishes
        // NodeContextCacheReadyEvent inside its @PostConstruct thread, which can fire
        // before TimeseriesBindingResumer's @EventListener is registered. With pre-
        // existing topics the bootstrap consumer assigns partitions and waits on poll,
        // giving Spring enough time to wire the listener before the event is published.
        if (!KAFKA.isRunning()) KAFKA.start();
        ensureTopicsStatic();
        reg.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        reg.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
        reg.add("prometheus-writer.remote-write.url", () -> mockRw.url("/api/v1/write").toString());
        // Speed batch flushes for the test
        reg.add("prometheus-writer.batch.max-interval-ms", () -> "200");
        reg.add("prometheus-writer.batch.max-samples", () -> "1");
        reg.add("prometheus-writer.labels.from-metadata[0]", () -> "snmp:sysContact");
    }

    private static void ensureTopicsStatic() {
        Properties p = new Properties();
        p.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient a = AdminClient.create(p)) {
            try {
                a.createTopics(List.of(
                        new NewTopic("deltav-node-context", 8, (short) 1)
                                .configs(Map.of("cleanup.policy", "compact")),
                        new NewTopic("deltav-timeseries", 8, (short) 1)))
                        .all().get();
            } catch (Exception e) {
                if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Autowired MeterRegistry metrics;

    @Test
    void end_to_end_sample_lands_at_rw_target() throws Exception {
        publishNodeContext(5, "Default", "server-01", "node-server-01",
                List.of("production", "critical"),
                Map.of("snmp:sysContact", "noc@example.com"));
        // Wait for cache to bootstrap + binding to resume
        Thread.sleep(3000);
        mockRw.enqueue(new MockResponse().setResponseCode(200));
        publishTimeseriesBatch(5, "Default", "mib2-interface-errors", "ifInDiscards",
                AttributeType.ATTRIBUTE_TYPE_COUNTER, 42.0);

        RecordedRequest req = mockRw.takeRequest(15, TimeUnit.SECONDS);
        assertThat(req).isNotNull();
        assertThat(req.getHeader("Content-Type")).isEqualTo("application/x-protobuf");
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");

        byte[] body = req.getBody().readByteArray();
        byte[] uncompressed = Snappy.uncompress(body);
        WriteRequest wr = WriteRequest.parseFrom(uncompressed);
        assertThat(wr.getTimeseriesCount()).isEqualTo(1);
        TimeSeries ts = wr.getTimeseries(0);
        Map<String, String> labels = ts.getLabelsList().stream()
                .collect(Collectors.toMap(Label::getName, Label::getValue));
        assertThat(labels).containsEntry("__name__", "opennms_mib2_interface_errors_ifindiscards_total");
        assertThat(labels).containsEntry("node_id", "5");
        assertThat(labels).containsEntry("node_label", "server-01");
        assertThat(labels).containsEntry("location", "Default");
        assertThat(labels).containsEntry("categories", "critical,production");
        assertThat(labels).containsEntry("instance", "server-01");
        assertThat(labels).containsEntry("foreign_source", "provision-prod");
        assertThat(labels).containsEntry("foreign_id", "node-server-01");
        assertThat(labels).containsEntry("snmp_syscontact", "noc@example.com");
        assertThat(ts.getSamples(0).getValue()).isEqualTo(42.0);
    }

    // --- helpers ---
    private void publishNodeContext(int nodeId, String location, String label, String foreignId,
                                    List<String> categories, Map<String, String> metadata) throws Exception {
        NodeContext.Builder b = NodeContext.newBuilder()
                .setNodeId(nodeId).setLocation(location).setNodeLabel(label)
                .setForeignSource("provision-prod")
                .setForeignId(foreignId)
                .addAllCategories(categories)
                .setUpdatedAtMs(System.currentTimeMillis());
        b.putAllMetadata(metadata);
        try (KafkaProducer<String, byte[]> prod = newProducer()) {
            prod.send(new ProducerRecord<>("deltav-node-context", location + "@" + nodeId, b.build().toByteArray())).get();
        }
    }

    private void publishTimeseriesBatch(int nodeId, String location, String groupName, String attrName,
                                        AttributeType type, double value) throws Exception {
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(nodeId).setLocation(location).setProducer(ProducerType.PRODUCER_COLLECTD)
                .setTimestampMs(1_700_000_000_000L)
                .setCollectionPackage("test-package")
                .addResources(Resource.newBuilder().setType("node").setInstance("")
                        .addGroups(AttributeGroup.newBuilder().setName(groupName)
                                .addAttributes(Attribute.newBuilder().setName(attrName).setType(type).setNumeric(value))))
                .build();
        try (KafkaProducer<String, byte[]> prod = newProducer()) {
            prod.send(new ProducerRecord<>("deltav-timeseries", location + "@" + nodeId, batch.toByteArray())).get();
        }
    }

    private KafkaProducer<String, byte[]> newProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(p);
    }
}
