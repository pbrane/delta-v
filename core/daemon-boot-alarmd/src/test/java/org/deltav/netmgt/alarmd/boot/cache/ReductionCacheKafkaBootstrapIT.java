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
package org.deltav.netmgt.alarmd.boot.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Boots a minimal Spring context with {@link ReductionCache} +
 * {@link ReductionCacheKafkaBootstrap} against a real Kafka container.
 * Pre-seeds the compacted topic with two non-tombstone records and one
 * tombstone (for a different key), then asserts:
 *
 * <ul>
 *   <li>{@link ReductionCacheReadyEvent} fires exactly once</li>
 *   <li>The cache contains the two live keys</li>
 *   <li>The tombstone key is absent from the cache</li>
 *   <li>The ready event reports the correct cache size (2)</li>
 * </ul>
 *
 * <p>The Spring context is deliberately limited to just the cache machinery
 * (no Alarmd, AlarmPersister, or KafkaAlarmPublisher) so the IT is fast and
 * isolated. The {@code cache-enabled=true} property override is supplied via
 * both {@code @SpringBootTest.properties} (high precedence) and
 * {@code @DynamicPropertySource} so it wins over the {@code application.yml}
 * default of {@code false}.
 *
 * <p>Wall clock: ~30-60s with warm Testcontainers image cache.
 */
@SpringBootTest(
        classes = {
            ReductionCacheKafkaBootstrapIT.TestContext.class,
            ReductionCache.class,
            ReductionCacheKafkaBootstrap.class
        },
        properties = {
            "deltav.alarmd.persistence.cache-enabled=true"
        })
@Testcontainers
class ReductionCacheKafkaBootstrapIT {

    private static final String TOPIC = "deltav-alarms-state-change";

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    static final AtomicInteger readyEventCount = new AtomicInteger();
    static final AtomicInteger readyCacheSize = new AtomicInteger(-1);

    @BeforeAll
    static void preloadTopic() throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(TOPIC, "rk-a",
                    AlarmState.newBuilder()
                            .setReductionKey("rk-a").setUei("u/a")
                            .setSeverity(AlarmState.Severity.MAJOR)
                            .setCount(1).build().toByteArray())).get();
            producer.send(new ProducerRecord<>(TOPIC, "rk-b",
                    AlarmState.newBuilder()
                            .setReductionKey("rk-b").setUei("u/b")
                            .setSeverity(AlarmState.Severity.MINOR)
                            .setCount(1).build().toByteArray())).get();
            // Tombstone for a different key — should not appear in the cache.
            producer.send(new ProducerRecord<>(TOPIC, "rk-c", null)).get();
        }
    }

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("deltav.alarmd.kafka-publisher.topic", () -> TOPIC);
        r.add("deltav.alarmd.kafka-publisher.bootstrap-servers", KAFKA::getBootstrapServers);
        // Override the alarmd test default of cache-enabled=false; the bootstrap
        // loader is @ConditionalOnProperty(name = "cache-enabled", havingValue = "true").
        r.add("deltav.alarmd.persistence.cache-enabled", () -> "true");
    }

    @Autowired
    ReductionCache cache;

    @Test
    void bootstrapReplayPopulatesCacheAndFiresReadyEvent() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(cache.isReady()).isTrue();
            assertThat(readyEventCount.get()).isEqualTo(1);
        });
        assertThat(cache.get("rk-a")).isPresent();
        assertThat(cache.get("rk-a").get().getUei()).isEqualTo("u/a");
        assertThat(cache.get("rk-b")).isPresent();
        assertThat(cache.get("rk-b").get().getUei()).isEqualTo("u/b");
        // Tombstone in topic → absent in cache.
        assertThat(cache.get("rk-c")).isEmpty();
        assertThat(cache.size()).isEqualTo(2);
        assertThat(readyCacheSize.get()).isEqualTo(2);
    }

    @Configuration
    static class TestContext {
        @Bean
        public ApplicationListener<ReductionCacheReadyEvent> readyListener() {
            return event -> {
                readyEventCount.incrementAndGet();
                readyCacheSize.set(event.getCacheSize());
            };
        }
    }
}
