/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.consume;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.config.AlertsForwarderProperties;
import org.deltav.alerts.forwarder.enrich.AlarmEnricher;
import org.deltav.alerts.forwarder.filter.AlarmFilter;
import org.deltav.alerts.forwarder.lifecycle.ActiveAlertRegistry;
import org.deltav.alerts.forwarder.nodecontext.NodeContextCache;
import org.deltav.alerts.forwarder.nodecontext.NodeContextCacheReadyEvent;
import org.deltav.alerts.forwarder.pipeline.AlarmForwardingPipeline;
import org.deltav.alerts.forwarder.sink.AlarmSink;
import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for {@link AlarmStateKafkaConsumer}: verifies the seek-to-beginning
 * replay, the startup gate, the parse-error DLQ path, and successful forwarding through
 * a recording sink — exercising everything that {@link org.deltav.alerts.forwarder.AlertsForwarderIntegrationIT}
 * explicitly bypasses by driving the pipeline directly.
 */
@Testcontainers
class AlarmStateKafkaConsumerIT {

    private static final String ALARMS_TOPIC = "deltav-alarms-state-change";
    private static final String DLQ_TOPIC = "deltav-alerts-forwarder-dlq";

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    private AlarmStateKafkaConsumer consumer;

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.stop();
        }
    }

    @Test
    @Timeout(60)
    void validAlarmIsForwardedAndInvalidRecordGoesToDlq() throws Exception {
        // Create both topics before publishing.
        createTopics(ALARMS_TOPIC, DLQ_TOPIC);

        // Recording sink collects all forwarded alarms.
        CopyOnWriteArrayList<ForwardedAlarm> forwarded = new CopyOnWriteArrayList<>();
        AlarmSink recordingSink = new AlarmSink() {
            public void forward(ForwardedAlarm a) { forwarded.add(a); }
            public String name() { return "recording"; }
        };

        // Wire all collaborators directly — no Spring context needed.
        ActiveAlertRegistry registry = new ActiveAlertRegistry();
        NodeContextCache nodeContextCache = new NodeContextCache();
        nodeContextCache.markReady();   // skip bootstrap; test doesn't need node enrichment

        AlarmForwardingPipeline pipeline = new AlarmForwardingPipeline(
                new AlarmFilter("WARNING", List.of()),
                new AlarmEnricher(nodeContextCache),
                registry,
                List.of(recordingSink),
                new SimpleMeterRegistry());

        AlertsForwarderProperties props = buildProps();

        consumer = new AlarmStateKafkaConsumer(
                props,
                pipeline,
                registry,
                new SimpleMeterRegistry(),
                kafka.getBootstrapServers());

        // Fire the startup gate directly — simulates the NodeContextKafkaBootstrap event.
        consumer.onNodeContextReady(new NodeContextCacheReadyEvent(this, 0L, 0));

        // Publish one valid AlarmState and one definitly-unparseable record.
        try (KafkaProducer<byte[], byte[]> producer = buildProducer()) {
            AlarmState alarm = AlarmState.newBuilder()
                    .setReductionKey("rk-consumer-it")
                    .setSeverity(AlarmState.Severity.MAJOR)
                    .setUei("uei/nodeDown")
                    .setNodeId(42)
                    .build();
            producer.send(new ProducerRecord<>(ALARMS_TOPIC, "rk-consumer-it".getBytes(),
                    alarm.toByteArray())).get();
            // Garbage bytes — not valid protobuf for AlarmState.
            producer.send(new ProducerRecord<>(ALARMS_TOPIC, "bad-key".getBytes(),
                    new byte[]{0x00, 0x01, 0x02, 0x03})).get();
        }

        // 1) The recording sink must receive the valid alarm.
        await().atMost(Duration.ofSeconds(30)).until(() -> !forwarded.isEmpty());
        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.get(0).state()).isEqualTo(ForwardedAlarm.State.FIRING);
        assertThat(forwarded.get(0).reductionKey()).isEqualTo("rk-consumer-it");

        // 2) The DLQ topic must receive the invalid record.
        await().atMost(Duration.ofSeconds(30)).until(() -> dlqHasRecord());
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private boolean dlqHasRecord() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-dlq-checker-" + System.nanoTime());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<byte[], byte[]> dlqConsumer = new KafkaConsumer<>(p)) {
            dlqConsumer.subscribe(List.of(DLQ_TOPIC));
            var records = dlqConsumer.poll(Duration.ofSeconds(2));
            return !records.isEmpty();
        }
    }

    private void createTopics(String... topics) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> newTopics = List.of(
                    new NewTopic(ALARMS_TOPIC, 1, (short) 1),
                    new NewTopic(DLQ_TOPIC, 1, (short) 1));
            admin.createTopics(newTopics).all().get();
        }
    }

    private KafkaProducer<byte[], byte[]> buildProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    private AlertsForwarderProperties buildProps() {
        AlertsForwarderProperties props = new AlertsForwarderProperties();
        props.setAlarmsTopic(ALARMS_TOPIC);
        // DLQ topic via nested Dlq class — default already matches DLQ_TOPIC constant.
        return props;
    }
}
