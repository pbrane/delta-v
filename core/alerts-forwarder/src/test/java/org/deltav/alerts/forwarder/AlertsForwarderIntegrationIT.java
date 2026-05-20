/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.enrich.AlarmEnricher;
import org.deltav.alerts.forwarder.filter.AlarmFilter;
import org.deltav.alerts.forwarder.lifecycle.ActiveAlertRegistry;
import org.deltav.alerts.forwarder.nodecontext.NodeContextCache;
import org.deltav.alerts.forwarder.pipeline.AlarmForwardingPipeline;
import org.deltav.alerts.forwarder.sink.AlarmSink;
import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class AlertsForwarderIntegrationIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Test
    @Timeout(60)
    void firingThenTombstoneProducesFiringThenResolved() throws Exception {
        String topic = "deltav-alarms-state-change";
        CopyOnWriteArrayList<ForwardedAlarm> sent = new CopyOnWriteArrayList<>();
        AlarmSink sink = new AlarmSink() {
            public void forward(ForwardedAlarm a) { sent.add(a); }
            public String name() { return "test"; }
        };

        AlarmForwardingPipeline pipeline = new AlarmForwardingPipeline(
                new AlarmFilter("WARNING", List.of()),
                new AlarmEnricher(new NodeContextCache()),
                new ActiveAlertRegistry(),
                List.of(sink),
                new SimpleMeterRegistry());

        // Publish a firing alarm, then a tombstone, both keyed by reduction key.
        try (KafkaProducer<byte[], byte[]> producer = producer()) {
            AlarmState alarm = AlarmState.newBuilder()
                    .setReductionKey("rk-1").setSeverity(AlarmState.Severity.MAJOR)
                    .setUei("uei/nodeDown").setNodeId(1).build();
            producer.send(new ProducerRecord<>(topic, "rk-1".getBytes(), alarm.toByteArray())).get();
            producer.send(new ProducerRecord<>(topic, "rk-1".getBytes(), null)).get();
        }

        // Drain the topic through the pipeline, mirroring AlarmStateKafkaConsumer.handle.
        try (var consumer = consumer()) {
            consumer.subscribe(List.of(topic));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                var records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(r -> pipeline.onRecord(
                        new String(r.key()),
                        r.value() == null ? null : parse(r.value())));
                return sent.size() >= 2;
            });
        }

        assertThat(sent).hasSize(2);
        assertThat(sent.get(0).state()).isEqualTo(ForwardedAlarm.State.FIRING);
        assertThat(sent.get(1).state()).isEqualTo(ForwardedAlarm.State.RESOLVED);
        assertThat(sent.get(1).labels()).isEqualTo(sent.get(0).labels());
    }

    private static AlarmState parse(byte[] bytes) {
        try {
            return AlarmState.parseFrom(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static KafkaProducer<byte[], byte[]> producer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(p);
    }

    private static org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer() {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("group.id", "it-" + System.nanoTime());
        p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("auto.offset.reset", "earliest");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(p);
    }
}
