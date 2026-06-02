/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.consume;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
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
import org.deltav.nodecontext.NodeContextCache;
import org.deltav.nodecontext.NodeContextCacheReadyEvent;
import org.deltav.alerts.forwarder.pipeline.AlarmForwardingPipeline;
import org.deltav.alerts.forwarder.sink.AlarmSink;
import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
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

    /**
     * A sink that throws with a 4xx (POISON) cause on the first alarm, then
     * records subsequent alarms normally. Verifies that:
     * <ol>
     *   <li>The poison record is DLQ'd and the consumer advances past it.</li>
     *   <li>A second alarm published after the poison one is forwarded normally.</li>
     * </ol>
     *
     * <p>Uses isolated topic names to avoid cross-test contamination from the
     * {@code seekToBeginning} replay design (all tests share the same static
     * Kafka container).</p>
     */
    @Test
    @Timeout(90)
    void poisonRecord4xxGoesToDlqAndConsumerAdvances() throws Exception {
        String alarmsTopic = "alarms-poison-" + System.nanoTime();
        String dlqTopic    = "dlq-poison-"    + System.nanoTime();
        createTopics(alarmsTopic, dlqTopic);

        CopyOnWriteArrayList<ForwardedAlarm> forwarded = new CopyOnWriteArrayList<>();
        AtomicInteger sinkCallCount = new AtomicInteger(0);

        // Sink throws HttpClientErrorException (4xx) on the first call, succeeds thereafter.
        AlarmSink poisonThenOkSink = new AlarmSink() {
            public void forward(ForwardedAlarm a) throws Exception {
                if (sinkCallCount.incrementAndGet() == 1) {
                    throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Bad Request");
                }
                forwarded.add(a);
            }
            public String name() { return "poison-test-sink"; }
        };

        ActiveAlertRegistry registry = new ActiveAlertRegistry();
        NodeContextCache nodeContextCache = new NodeContextCache();
        nodeContextCache.markReady();

        AlarmForwardingPipeline pipeline = new AlarmForwardingPipeline(
                new AlarmFilter("WARNING", List.of()),
                new AlarmEnricher(nodeContextCache),
                registry,
                List.of(poisonThenOkSink),
                new SimpleMeterRegistry());

        consumer = new AlarmStateKafkaConsumer(
                buildProps(alarmsTopic, dlqTopic), pipeline, registry, new SimpleMeterRegistry(),
                kafka.getBootstrapServers());
        consumer.onNodeContextReady(new NodeContextCacheReadyEvent(this, 0L, 0));

        try (KafkaProducer<byte[], byte[]> producer = buildProducer()) {
            // First alarm — the sink will 4xx this one (poison).
            AlarmState poisonAlarm = AlarmState.newBuilder()
                    .setReductionKey("rk-poison")
                    .setSeverity(AlarmState.Severity.MAJOR)
                    .setUei("uei/poison")
                    .setNodeId(1)
                    .build();
            producer.send(new ProducerRecord<>(alarmsTopic, "rk-poison".getBytes(),
                    poisonAlarm.toByteArray())).get();

            // Second alarm — should be forwarded normally after the poison is DLQ'd.
            AlarmState goodAlarm = AlarmState.newBuilder()
                    .setReductionKey("rk-good")
                    .setSeverity(AlarmState.Severity.MAJOR)
                    .setUei("uei/good")
                    .setNodeId(2)
                    .build();
            producer.send(new ProducerRecord<>(alarmsTopic, "rk-good".getBytes(),
                    goodAlarm.toByteArray())).get();
        }

        // The good alarm must eventually be forwarded — meaning the consumer advanced past the poison.
        await().atMost(Duration.ofSeconds(60)).until(() -> !forwarded.isEmpty());
        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.get(0).reductionKey()).isEqualTo("rk-good");

        // The DLQ must have received exactly the poison record.
        await().atMost(Duration.ofSeconds(30)).until(() -> dlqHasRecord(dlqTopic));
    }

    /**
     * A sink that throws with a 5xx (TRANSIENT) cause on the first attempt then
     * succeeds on retry. Verifies that:
     * <ol>
     *   <li>No DLQ record is published.</li>
     *   <li>The alarm IS forwarded after the retry.</li>
     * </ol>
     *
     * <p>Uses isolated topic names to avoid cross-test contamination from the
     * {@code seekToBeginning} replay design.</p>
     */
    @Test
    @Timeout(90)
    void transientRecord5xxIsRetriedAndForwarded() throws Exception {
        String alarmsTopic = "alarms-transient-" + System.nanoTime();
        String dlqTopic    = "dlq-transient-"    + System.nanoTime();
        createTopics(alarmsTopic, dlqTopic);

        CopyOnWriteArrayList<ForwardedAlarm> forwarded = new CopyOnWriteArrayList<>();
        AtomicInteger sinkCallCount = new AtomicInteger(0);

        // Sink throws HttpServerErrorException (5xx) on the first call, succeeds on second.
        AlarmSink transientThenOkSink = new AlarmSink() {
            public void forward(ForwardedAlarm a) throws Exception {
                if (sinkCallCount.incrementAndGet() == 1) {
                    throw new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE, "Service Unavailable");
                }
                forwarded.add(a);
            }
            public String name() { return "transient-test-sink"; }
        };

        ActiveAlertRegistry registry = new ActiveAlertRegistry();
        NodeContextCache nodeContextCache = new NodeContextCache();
        nodeContextCache.markReady();

        AlarmForwardingPipeline pipeline = new AlarmForwardingPipeline(
                new AlarmFilter("WARNING", List.of()),
                new AlarmEnricher(nodeContextCache),
                registry,
                List.of(transientThenOkSink),
                new SimpleMeterRegistry());

        consumer = new AlarmStateKafkaConsumer(
                buildProps(alarmsTopic, dlqTopic), pipeline, registry, new SimpleMeterRegistry(),
                kafka.getBootstrapServers());
        consumer.onNodeContextReady(new NodeContextCacheReadyEvent(this, 0L, 0));

        try (KafkaProducer<byte[], byte[]> producer = buildProducer()) {
            AlarmState alarm = AlarmState.newBuilder()
                    .setReductionKey("rk-transient")
                    .setSeverity(AlarmState.Severity.MAJOR)
                    .setUei("uei/transient")
                    .setNodeId(3)
                    .build();
            producer.send(new ProducerRecord<>(alarmsTopic, "rk-transient".getBytes(),
                    alarm.toByteArray())).get();
        }

        // The alarm must eventually be forwarded after the retry.
        await().atMost(Duration.ofSeconds(60)).until(() -> !forwarded.isEmpty());
        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.get(0).reductionKey()).isEqualTo("rk-transient");

        // No DLQ record should have been published for a transient failure.
        // The isolated DLQ topic is fresh — any record there is an error.
        assertThat(dlqHasRecord(dlqTopic)).isFalse();
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private boolean dlqHasRecord() {
        return dlqHasRecord(DLQ_TOPIC);
    }

    private boolean dlqHasRecord(String dlqTopic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-dlq-checker-" + System.nanoTime());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<byte[], byte[]> dlqConsumer = new KafkaConsumer<>(p)) {
            dlqConsumer.subscribe(List.of(dlqTopic));
            var records = dlqConsumer.poll(Duration.ofSeconds(2));
            return !records.isEmpty();
        }
    }

    private void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            // Create each topic individually so a TopicExistsException on one topic
            // (when tests share the same static Kafka container) does not prevent the
            // others from being created.
            for (String name : topicNames) {
                try {
                    admin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get();
                } catch (java.util.concurrent.ExecutionException ex) {
                    if (!(ex.getCause() instanceof TopicExistsException)) {
                        throw ex;
                    }
                    // Topic already exists — that is fine; nothing to do.
                }
            }
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
        return buildProps(ALARMS_TOPIC, DLQ_TOPIC);
    }

    private AlertsForwarderProperties buildProps(String alarmsTopic, String dlqTopic) {
        AlertsForwarderProperties props = new AlertsForwarderProperties();
        props.setAlarmsTopic(alarmsTopic);
        props.getDlq().setTopic(dlqTopic);
        return props;
    }
}
