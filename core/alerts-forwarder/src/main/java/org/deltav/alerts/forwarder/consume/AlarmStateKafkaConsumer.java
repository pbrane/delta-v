/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.consume;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.deltav.alarms.proto.AlarmState;
import org.deltav.alerts.forwarder.config.AlertsForwarderProperties;
import org.deltav.alerts.forwarder.dlq.DlqPublisher;
import org.deltav.alerts.forwarder.lifecycle.ActiveAlertRegistry;
import org.deltav.alerts.forwarder.metrics.AlertsForwarderMetrics;
import org.deltav.alerts.forwarder.nodecontext.NodeContextCacheReadyEvent;
import org.deltav.alerts.forwarder.pipeline.AlarmForwardingPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Dedicated bootstrap-replay consumer for {@code deltav-alarms-state-change}.
 * Starts only after {@link NodeContextCacheReadyEvent} (the §5.5 startup gate),
 * seeks to the beginning, and feeds every record to {@link AlarmForwardingPipeline}.
 * Because the topic is compacted, the replay rebuilds {@link ActiveAlertRegistry}
 * to the exact current set, then the loop live-tails.
 *
 * <p>A value that fails {@code AlarmState.parseFrom} is DLQ'd and skipped. A
 * sink failure ({@code SinkForwardException}) makes the thread sleep and retry
 * the same record — back-pressure, no alarm dropped.</p>
 */
@Component
public class AlarmStateKafkaConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmStateKafkaConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(5);

    private final AlertsForwarderProperties props;
    private final AlarmForwardingPipeline pipeline;
    private final DlqPublisher dlq;
    private final String bootstrapServers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread thread;

    public AlarmStateKafkaConsumer(AlertsForwarderProperties props,
                                   AlarmForwardingPipeline pipeline,
                                   ActiveAlertRegistry registry,
                                   MeterRegistry meterRegistry,
                                   @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.props = props;
        this.pipeline = pipeline;
        this.dlq = new DlqPublisher(buildProducer(bootstrapServers), props.getDlq().getTopic(), meterRegistry);
        this.bootstrapServers = bootstrapServers;
        Gauge.builder(AlertsForwarderMetrics.ACTIVE_ALERTS, registry, ActiveAlertRegistry::size)
                .register(meterRegistry);
    }

    /** §5.5 startup gate: do not consume alarms until node context is materialized. */
    @EventListener
    public void onNodeContextReady(NodeContextCacheReadyEvent event) {
        if (running.compareAndSet(false, true)) {
            LOG.info("NodeContextCache ready (size={}) — starting alarm consumer", event.getCacheSize());
            thread = new Thread(this, "alarm-state-consumer");
            thread.setDaemon(true);
            thread.start();
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (thread != null) {
            try {
                thread.join(5000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void run() {
        try (KafkaConsumer<byte[], byte[]> consumer = buildConsumer()) {
            consumer.subscribe(List.of(props.getAlarmsTopic()));
            // Compacted topic: replay from the beginning every start to rebuild
            // the active-alert registry. seekToBeginning on first assignment.
            boolean seeked = false;
            while (running.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
                if (!seeked && !consumer.assignment().isEmpty()) {
                    consumer.seekToBeginning(consumer.assignment());
                    seeked = true;
                    continue;
                }
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    handleWithRetry(record);
                }
            }
        } catch (Exception e) {
            LOG.error("AlarmStateKafkaConsumer fatal error", e);
        }
    }

    private void handleWithRetry(ConsumerRecord<byte[], byte[]> record) {
        while (running.get()) {
            try {
                handle(record);
                return;
            } catch (AlarmForwardingPipeline.SinkForwardException e) {
                LOG.warn("Sink failure — retrying record in {}s", RETRY_BACKOFF.toSeconds(), e);
                sleep(RETRY_BACKOFF);
            }
        }
    }

    private void handle(ConsumerRecord<byte[], byte[]> record) {
        String reductionKey = record.key() == null ? "" : new String(record.key());
        if (record.value() == null) {
            pipeline.onRecord(reductionKey, null);   // Kafka tombstone — alarm deleted.
            return;
        }
        AlarmState alarm;
        try {
            alarm = AlarmState.parseFrom(record.value());
        } catch (Exception e) {
            LOG.warn("Unparseable AlarmState key={} offset={} — DLQ", reductionKey, record.offset(), e);
            dlq.publish(record.key(), record.value(), "parse-error");
            return;
        }
        pipeline.onRecord(reductionKey, alarm);
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private KafkaConsumer<byte[], byte[]> buildConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "alerts-forwarder-alarms-" + UUID.randomUUID());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(p);
    }

    private static KafkaProducer<byte[], byte[]> buildProducer(String bootstrapServers) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "alerts-forwarder-dlq");
        return new KafkaProducer<>(p);
    }
}
