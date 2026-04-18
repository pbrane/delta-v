/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.timeseries.proto.NodeContext;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for {@link NodeContextKafkaBootstrap}: drives a real Kafka broker
 * via Testcontainers, asserts the bootstrap drains to HWM and tombstones propagate
 * during the live-tail loop.
 *
 * <p>Pinned to {@code apache/kafka:3.8.0} — same image as the sibling Phase 1
 * {@code NodeContextKafkaIT} in daemon-boot-provisiond for layer-cache reuse.
 */
@Testcontainers
class NodeContextKafkaBootstrapIT {

    private static final String TOPIC = "deltav-node-context";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0"))
            .withStartupTimeout(Duration.ofSeconds(120));

    @Test
    void bootstrap_drains_to_HWM_then_marks_ready() throws Exception {
        ensureTopic();
        for (int i = 1; i <= 50; i++) {
            produce(i, false);
        }

        NodeContextCache cache = new NodeContextCache();
        AtomicReference<NodeContextCacheReadyEvent> captured = new AtomicReference<>();
        ApplicationEventPublisher publisher = event -> {
            if (event instanceof NodeContextCacheReadyEvent r) {
                captured.set(r);
            }
        };
        NodeContextKafkaBootstrap boot = new NodeContextKafkaBootstrap(
                cache, publisher, new SimpleMeterRegistry(), KAFKA.getBootstrapServers());
        boot.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(cache::isReady);
            assertThat(cache.size()).isEqualTo(50);
            assertThat(captured.get()).isNotNull();
            assertThat(captured.get().getCacheSize()).isEqualTo(50);
        } finally {
            boot.stop();
        }
    }

    @Test
    void tombstone_removes_key_from_cache_live() throws Exception {
        ensureTopic();
        produce(101, false);

        NodeContextCache cache = new NodeContextCache();
        ApplicationEventPublisher publisher = e -> { };
        NodeContextKafkaBootstrap boot = new NodeContextKafkaBootstrap(
                cache, publisher, new SimpleMeterRegistry(), KAFKA.getBootstrapServers());
        boot.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(cache::isReady);
            assertThat(cache.get("Default@101")).isPresent();

            produce(101, true);   // tombstone (deleted=true)
            await().atMost(Duration.ofSeconds(20))
                    .until(() -> cache.get("Default@101").isEmpty());
        } finally {
            boot.stop();
        }
    }

    // -------------------- helpers --------------------

    private void ensureTopic() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient a = AdminClient.create(p)) {
            a.createTopics(List.of(new NewTopic(TOPIC, 8, (short) 1)
                            .configs(Map.of("cleanup.policy", "compact"))))
                    .all().get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw e;
            }
        }
    }

    private void produce(int nodeId, boolean deleted) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<String, byte[]> prod = new KafkaProducer<>(p)) {
            NodeContext nc = NodeContext.newBuilder()
                    .setNodeId(nodeId)
                    .setLocation("Default")
                    .setNodeLabel("node-" + nodeId)
                    .setDeleted(deleted)
                    .setUpdatedAtMs(System.currentTimeMillis())
                    .build();
            prod.send(new ProducerRecord<>(TOPIC, "Default@" + nodeId, nc.toByteArray())).get();
        }
    }
}
