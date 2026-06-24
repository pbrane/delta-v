/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Testcontainers end-to-end integration test for the alarms-materializer.
 * Validates the full pipeline: Kafka producer -> consumer -> projector ->
 * PG upsert / delete, plus the retention engine's tombstone-then-delete loop.
 *
 * <p>Wall clock: ~60-120s on first run (image pulls); subsequent runs are
 * faster as Testcontainers caches the images.
 */
@SpringBootTest(classes = AlarmsMaterializerApplication.class,
        properties = {
            "deltav.alarms-materializer.retention.cadence=PT2S",
            "deltav.alarms-materializer.retention.initial-delay=PT2S",
            "deltav.alarms-materializer.retention.rules[0].id=test-gc",
            "deltav.alarms-materializer.retention.rules[0].action=delete",
            "deltav.alarms-materializer.retention.rules[0].when.idle_for=PT1S"
        })
@Testcontainers
class AlarmsMaterializerIT {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("opennms")
            .withUsername("opennms")
            .withPassword("opennms")
            .withInitScript("schema/alarms.sql");

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.datasource.url", PG::getJdbcUrl);
        r.add("spring.datasource.username", PG::getUsername);
        r.add("spring.datasource.password", PG::getPassword);
    }

    @Autowired JdbcTemplate jdbc;

    @Test
    void upsertProjectsProtoToPgRow() throws Exception {
        try (KafkaProducer<String, byte[]> producer = buildProducer()) {
            AlarmState alarm = AlarmState.newBuilder()
                    .setReductionKey("rk-1").setUei("uei/x")
                    .setSeverity(AlarmState.Severity.MAJOR)
                    .setNodeId(42).setLocation("Default")
                    .setFirstEventTimeMs(1_700_000_000_000L)
                    .setLastEventTimeMs(System.currentTimeMillis() + 10 * 60 * 1000)  // beyond retention window
                    .setCount(1).setServiceName("ICMP").build();
            producer.send(new ProducerRecord<>("deltav-alarms-state-change",
                    "rk-1", alarm.toByteArray())).get();

            await().atMost(Duration.ofSeconds(20)).ignoreExceptions().untilAsserted(() -> {
                Map<String, Object> row = jdbc.queryForMap(
                        "SELECT eventuei, nodeid, severity, reductionkey, serviceid " +
                        "FROM alarms WHERE reductionkey = ?", "rk-1");
                assertThat(row.get("eventuei")).isEqualTo("uei/x");
                assertThat(row.get("nodeid")).isEqualTo(42);
                assertThat(row.get("severity")).isEqualTo(AlarmState.Severity.MAJOR.getNumber());
                assertThat(row.get("serviceid")).isNotNull();
            });
        }
    }

    @Test
    void republishIsIdempotent() throws Exception {
        try (KafkaProducer<String, byte[]> producer = buildProducer()) {
            AlarmState first = AlarmState.newBuilder()
                    .setReductionKey("rk-2").setUei("uei/x").setSeverity(AlarmState.Severity.MINOR)
                    .setNodeId(43).setLocation("Default")
                    .setLastEventTimeMs(System.currentTimeMillis() + 60_000)
                    .setCount(1).build();
            producer.send(new ProducerRecord<>("deltav-alarms-state-change",
                    "rk-2", first.toByteArray())).get();

            await().atMost(Duration.ofSeconds(20)).until(() ->
                jdbc.queryForList("SELECT 1 FROM alarms WHERE reductionkey = ?", "rk-2").size() == 1);
            Integer alarmidFirst = jdbc.queryForObject(
                    "SELECT alarmid FROM alarms WHERE reductionkey = ?", Integer.class, "rk-2");

            AlarmState second = first.toBuilder().setCount(2).build();
            producer.send(new ProducerRecord<>("deltav-alarms-state-change",
                    "rk-2", second.toByteArray())).get();

            await().atMost(Duration.ofSeconds(10)).ignoreExceptions().untilAsserted(() -> {
                Integer counter = jdbc.queryForObject(
                        "SELECT counter FROM alarms WHERE reductionkey = ?", Integer.class, "rk-2");
                assertThat(counter).isEqualTo(2);
            });
            Integer alarmidSecond = jdbc.queryForObject(
                    "SELECT alarmid FROM alarms WHERE reductionkey = ?", Integer.class, "rk-2");
            assertThat(alarmidSecond).isEqualTo(alarmidFirst);
        }
    }

    @Test
    void tombstoneDeletesRow() throws Exception {
        try (KafkaProducer<String, byte[]> producer = buildProducer()) {
            AlarmState alarm = AlarmState.newBuilder()
                    .setReductionKey("rk-3").setUei("uei/x").setSeverity(AlarmState.Severity.MAJOR)
                    .setNodeId(44).setLocation("Default")
                    .setLastEventTimeMs(System.currentTimeMillis() + 60_000)
                    .setCount(1).build();
            producer.send(new ProducerRecord<>("deltav-alarms-state-change",
                    "rk-3", alarm.toByteArray())).get();

            await().atMost(Duration.ofSeconds(20)).until(() ->
                jdbc.queryForList("SELECT 1 FROM alarms WHERE reductionkey = ?", "rk-3").size() == 1);

            producer.send(new ProducerRecord<>("deltav-alarms-state-change",
                    "rk-3", (byte[]) null)).get();

            await().atMost(Duration.ofSeconds(15)).until(() ->
                jdbc.queryForList("SELECT 1 FROM alarms WHERE reductionkey = ?", "rk-3").isEmpty());
        }
    }

    @Test
    void retentionRuleProducesTombstoneAndConsumerDeletes() throws Exception {
        try (KafkaProducer<String, byte[]> producer = buildProducer()) {
            // Old alarm — lasteventtime in the past so retention's idle_for: PT1S matches immediately.
            AlarmState alarm = AlarmState.newBuilder()
                    .setReductionKey("rk-4").setUei("uei/x").setSeverity(AlarmState.Severity.MINOR)
                    .setNodeId(45).setLocation("Default")
                    .setLastEventTimeMs(System.currentTimeMillis() - 10_000)
                    .setCount(1).build();
            producer.send(new ProducerRecord<>("deltav-alarms-state-change",
                    "rk-4", alarm.toByteArray())).get();

            await().atMost(Duration.ofSeconds(20)).until(() ->
                jdbc.queryForList("SELECT 1 FROM alarms WHERE reductionkey = ?", "rk-4").size() == 1);

            await().atMost(Duration.ofSeconds(60)).until(() ->
                jdbc.queryForList("SELECT 1 FROM alarms WHERE reductionkey = ?", "rk-4").isEmpty());
        }
    }

    private KafkaProducer<String, byte[]> buildProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        return new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
    }
}
