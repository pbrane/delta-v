/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.collectd.timeseries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectionSetVisitor;
import org.opennms.netmgt.collection.api.CollectionStatus;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.model.ResourcePath;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Layer 4 broker integration test: validates real Kafka wire behaviour.
 *
 * <p>Uses an {@link ApplicationContextInitializer} to start the
 * {@code apache/kafka:3.8.0} Testcontainers broker before the Spring Boot
 * context loads, avoiding the Spring Boot 4 / JUnit 5 ordering issue where
 * the Spring context bootstraps before the {@code @Testcontainers} extension
 * runs {@code beforeAll}.</p>
 *
 * <p>Pinned to {@code apache/kafka:3.8.0} for reproducible IT runs. The
 * delta-v Docker Compose uses {@code apache/kafka:latest} (same image family);
 * Kafka's wire protocol is backwards-compatible so the Layer 4 and Layer 5
 * brokers need not be identical.</p>
 */
@SpringBootTest(classes = TimeseriesKafkaBrokerIT.TestApp.class,
        properties = {
                "deltav.timeseries.enabled=true",
                "spring.main.web-application-type=none",
                "spring.main.banner-mode=off",
                "spring.cloud.stream.kafka.binder.auto-create-topics=false"
        })
@ContextConfiguration(initializers = TimeseriesKafkaBrokerIT.KafkaInitializer.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TimeseriesKafkaBrokerIT {

    /**
     * Shared Kafka container. Started once by {@link KafkaInitializer} before
     * the Spring context loads; stopped automatically on JVM exit via TC's
     * Ryuk reaper.
     */
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0"))
            .withStartupTimeout(Duration.ofSeconds(120));

    /**
     * {@code ApplicationContextInitializer} that starts the Kafka container
     * and injects {@code spring.cloud.stream.kafka.binder.brokers} before the
     * Spring Boot context is loaded. This sidesteps the ordering problem where
     * Spring Boot 4's test context bootstrapper runs before the JUnit 5
     * {@code TestcontainersExtension.beforeAll}.
     */
    static class KafkaInitializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext ctx) {
            if (!KAFKA.isRunning()) {
                KAFKA.start();
            }
            String brokers = KAFKA.getBootstrapServers();
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(ctx,
                    "spring.cloud.stream.kafka.binder.brokers=" + brokers,
                    "spring.kafka.bootstrap-servers=" + brokers);
        }
    }

    @Autowired
    TimeseriesKafkaPublisher publisher;

    @Test
    @Order(1)
    void roundTripWithLz4Compression() throws Exception {
        publisher.publish(stubSet(1), "default", 1, "Default");

        try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            consumer.subscribe(Collections.singletonList("deltav-timeseries"));
            ConsumerRecord<byte[], byte[]> record = awaitOne(consumer);
            TimeseriesBatch batch = TimeseriesBatch.parseFrom(record.value());
            assertThat(batch.getNodeId()).isEqualTo(1);
            assertThat(batch.getLocation()).isEqualTo("Default");
            assertThat(new String(record.key())).isEqualTo("Default@1");
        }
    }

    @Test
    @Order(2)
    void newTopicsAreProvisionedWithCorrectConfiguration() throws Exception {
        // Trigger publish first so Spring Kafka admin has a chance to provision topics
        publisher.publish(stubSet(1), "default", 1, "Default");
        Thread.sleep(500);

        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            TopicDescription ts = admin.describeTopics(
                    Collections.singletonList("deltav-timeseries")).allTopicNames().get()
                    .get("deltav-timeseries");
            assertThat(ts.partitions()).hasSize(16);

            ConfigResource tsResource = new ConfigResource(ConfigResource.Type.TOPIC, "deltav-timeseries");
            Config tsConfig = admin.describeConfigs(Collections.singletonList(tsResource))
                    .all().get().get(tsResource);
            assertThat(tsConfig.get("cleanup.policy").value()).isEqualTo("delete");
            assertThat(tsConfig.get("compression.type").value()).isEqualTo("lz4");
            assertThat(tsConfig.get("retention.ms").value())
                    .isEqualTo(String.valueOf(Duration.ofDays(1).toMillis()));
        }
    }

    @Test
    @Order(3)
    void partitionStickinessForSameKey() throws Exception {
        byte[] targetKey = "Default@7".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int i = 0; i < 5; i++) publisher.publish(stubSet(7), "default", 7, "Default");

        try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
            consumer.subscribe(Collections.singletonList("deltav-timeseries"));
            long deadline = System.currentTimeMillis() + 10_000;
            int partition = -1;
            int seen = 0;
            while (System.currentTimeMillis() < deadline && seen < 5) {
                ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : recs) {
                    // Only check records published by this test (key = "Default@7")
                    if (!java.util.Arrays.equals(r.key(), targetKey)) continue;
                    if (partition == -1) partition = r.partition();
                    else assertThat(r.partition()).isEqualTo(partition);
                    seen++;
                }
            }
            assertThat(seen).isGreaterThanOrEqualTo(5);
        }
    }

    private KafkaConsumer<byte[], byte[]> consumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "ts-test-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
    }

    private ConsumerRecord<byte[], byte[]> awaitOne(KafkaConsumer<byte[], byte[]> consumer) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<byte[], byte[]> r : recs) return r;
        }
        throw new AssertionError("no record received in 10 s");
    }

    private CollectionSet stubSet(int nodeId) {
        CollectionSet set = mock(CollectionSet.class);
        when(set.getStatus()).thenReturn(CollectionStatus.SUCCEEDED);
        when(set.getCollectionTimestamp()).thenReturn(new Date());
        org.mockito.Mockito.doAnswer(inv -> {
            CollectionSetVisitor v = inv.getArgument(0);
            CollectionResource res = mock(CollectionResource.class);
            when(res.getResourceTypeName()).thenReturn("node");
            when(res.getInstance()).thenReturn(null);
            when(res.getParent()).thenReturn(ResourcePath.get("node[" + nodeId + "]"));
            org.opennms.netmgt.collection.api.AttributeGroup g =
                    mock(org.opennms.netmgt.collection.api.AttributeGroup.class);
            when(g.getName()).thenReturn("mib2-system");
            CollectionAttribute a = mock(CollectionAttribute.class);
            when(a.getName()).thenReturn("sysUpTime");
            when(a.getType()).thenReturn(org.opennms.netmgt.collection.api.AttributeType.GAUGE);
            when(a.getNumericValue()).thenReturn(12345);
            v.visitCollectionSet(set);
            v.visitResource(res);
            v.visitGroup(g);
            v.visitAttribute(a);
            v.completeAttribute(a);
            v.completeGroup(g);
            v.completeResource(res);
            v.completeCollectionSet(set);
            return null;
        }).when(set).visit(org.mockito.ArgumentMatchers.any());
        return set;
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({TimeseriesKafkaPublisherConfiguration.class})
    static class TestApp {
        /**
         * Composite factory needs an inner @Qualifier("timeseriesPersisterFactory") bean.
         * Mock suffices for the broker IT — the real Timeseries persister path is
         * tested separately in unit tests.
         */
        @Bean(name = "timeseriesPersisterFactory")
        PersisterFactory stubInnerFactory() {
            return mock(PersisterFactory.class);
        }

        /**
         * M2 decorator's @Qualifier("locationAwareCollectorClient") references
         * horizon's bean from CollectdRpcConfiguration, which is not imported
         * here. Provide a mock under the expected name so the decorator can wrap
         * it; this IT doesn't exercise the collect() path.
         */
        @Bean(name = "locationAwareCollectorClient")
        org.opennms.netmgt.collection.api.LocationAwareCollectorClient innerCollectorClient() {
            return mock(org.opennms.netmgt.collection.api.LocationAwareCollectorClient.class);
        }
    }
}
