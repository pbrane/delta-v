/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(classes = PrometheusWriterApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CircuitBreakerIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"))
                    .withStartupTimeout(Duration.ofSeconds(120));

    static MockWebServer mockRw;

    @BeforeAll static void startMock() throws Exception { mockRw = new MockWebServer(); mockRw.start(); }
    @AfterAll  static void stopMock()  throws Exception { if (mockRw != null) mockRw.shutdown(); }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry reg) {
        // See RwRoundTripIT.props — topics MUST exist before Spring boots so
        // NodeContextCacheReadyEvent fires after TimeseriesBindingResumer is wired.
        if (!KAFKA.isRunning()) KAFKA.start();
        ensureTopicsStatic();
        reg.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        reg.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
        reg.add("prometheus-writer.remote-write.url", () -> mockRw.url("/api/v1/write").toString());
        reg.add("prometheus-writer.batch.max-interval-ms", () -> "100");
        reg.add("prometheus-writer.batch.max-samples", () -> "1");
        // Tight circuit timings for testability
        reg.add("prometheus-writer.circuit-breaker.failure-rate-threshold", () -> "50");
        reg.add("prometheus-writer.circuit-breaker.sliding-window-size", () -> "5");
        reg.add("prometheus-writer.circuit-breaker.minimum-number-of-calls", () -> "3");
        reg.add("prometheus-writer.circuit-breaker.wait-duration-open-ms", () -> "1500");
        reg.add("prometheus-writer.circuit-breaker.half-open-permitted-calls", () -> "1");
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

    @Autowired CircuitBreaker breaker;
    @Autowired MeterRegistry metrics;

    @Test
    void circuit_opens_on_failures_then_recovers() throws Exception {
        publishNodeContext(7, "Default", "server-7", List.of());
        Thread.sleep(3000);  // cache bootstrap + binding resume

        // Inject enough 500s to trip the breaker (sliding window 5, threshold 50%, min calls 3)
        for (int i = 0; i < 5; i++) mockRw.enqueue(new MockResponse().setResponseCode(500));
        for (int i = 0; i < 5; i++) {
            publishTimeseriesBatch(7, "Default", "test-group", "test-attr-" + i,
                    AttributeType.ATTRIBUTE_TYPE_GAUGE, i);
        }

        // Await: circuit OPEN
        await().atMost(Duration.ofSeconds(20))
                .until(() -> breaker.getState() == CircuitBreaker.State.OPEN);
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Enqueue a couple of success responses — harmless, in case the binding flushes a
        // residual buffered batch after recovery.
        for (int i = 0; i < 2; i++) mockRw.enqueue(new MockResponse().setResponseCode(200));

        // Wait past wait-duration-open-ms (1500ms) for the automatic OPEN→HALF_OPEN transition.
        await().atMost(Duration.ofSeconds(10))
                .until(() -> breaker.getState() == CircuitBreaker.State.HALF_OPEN
                        || breaker.getState() == CircuitBreaker.State.CLOSED);

        // Task 16's ConsumerPauseListener pauses the SCS binding on OPEN and only resumes
        // on HALF_OPEN→CLOSED — so half-open probes can't be driven by new Kafka messages.
        // In production, BatchingRwWriter's @Scheduled flushIfStale() drives the probe by
        // flushing a residual buffered batch through the breaker. The IT pre-emptively
        // cleared the buffer when the original failures happened, so we drive the half-open
        // probe directly. half-open-permitted-calls=1 → one successful call closes the breaker.
        breaker.executeCallable(() -> 200);

        await().atMost(Duration.ofSeconds(5))
                .until(() -> breaker.getState() == CircuitBreaker.State.CLOSED);
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    // --- helpers (same as RwRoundTripIT) ---
    private void publishNodeContext(int nodeId, String location, String label, List<String> categories) throws Exception {
        NodeContext nc = NodeContext.newBuilder()
                .setNodeId(nodeId).setLocation(location).setNodeLabel(label)
                .addAllCategories(categories)
                .setUpdatedAtMs(System.currentTimeMillis()).build();
        try (KafkaProducer<String, byte[]> prod = newProducer()) {
            prod.send(new ProducerRecord<>("deltav-node-context", location + "@" + nodeId, nc.toByteArray())).get();
        }
    }

    private void publishTimeseriesBatch(int nodeId, String location, String groupName, String attrName,
                                        AttributeType type, double value) throws Exception {
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(nodeId).setLocation(location).setProducer(ProducerType.PRODUCER_COLLECTD)
                .setTimestampMs(System.currentTimeMillis())
                .addResources(Resource.newBuilder().setType("node")
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
