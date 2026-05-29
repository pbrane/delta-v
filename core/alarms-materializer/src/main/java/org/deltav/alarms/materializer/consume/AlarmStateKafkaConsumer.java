/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.consume;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.deltav.alarms.materializer.config.MaterializerProperties;
import org.deltav.alarms.materializer.delete.AlarmDeleter;
import org.deltav.alarms.materializer.metrics.MaterializerMetrics;
import org.deltav.alarms.materializer.upsert.AlarmUpsertWriter;
import org.deltav.alarms.proto.AlarmState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * Bootstrap-replay raw {@code KafkaConsumer} for {@code deltav-alarms-state-change}.
 * Seeks to the beginning on first assignment to replay the compacted topic;
 * thereafter live-tails. Each non-tombstone record is upserted to PG; each
 * tombstone deletes by reduction key.
 *
 * <p>On a PG outage the upsert/delete throws a {@link DataAccessException}; the
 * consumer thread blocks retrying the same record (natural back-pressure — the
 * broker keeps the record, nothing is dropped).
 *
 * <p>The materializer has no node-context dependency, so it starts on
 * {@link ApplicationReadyEvent} — no startup gate.
 */
@Component
public class AlarmStateKafkaConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmStateKafkaConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(5);

    private final MaterializerProperties props;
    private final AlarmUpsertWriter upserter;
    private final AlarmDeleter deleter;
    private final MeterRegistry meterRegistry;
    private final String bootstrapServers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Counter upsertCounter;
    private final Counter deleteCounter;
    private final Counter skipCounter;
    private Thread thread;

    public AlarmStateKafkaConsumer(MaterializerProperties props,
                                   AlarmUpsertWriter upserter,
                                   AlarmDeleter deleter,
                                   MeterRegistry meterRegistry,
                                   @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.props = props;
        this.upserter = upserter;
        this.deleter = deleter;
        this.meterRegistry = meterRegistry;
        this.bootstrapServers = bootstrapServers;
        this.upsertCounter = meterRegistry.counter(MaterializerMetrics.RECORDS_CONSUMED, MaterializerMetrics.TAG_OUTCOME, "upsert");
        this.deleteCounter = meterRegistry.counter(MaterializerMetrics.RECORDS_CONSUMED, MaterializerMetrics.TAG_OUTCOME, "delete");
        this.skipCounter   = meterRegistry.counter(MaterializerMetrics.RECORDS_CONSUMED, MaterializerMetrics.TAG_OUTCOME, "skip");
    }

    @EventListener
    public void onReady(ApplicationReadyEvent event) {
        if (running.compareAndSet(false, true)) {
            LOG.info("Starting alarms-materializer consumer for topic {}", props.getAlarmsTopic());
            thread = new Thread(this, "alarms-materializer-consumer");
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
            } catch (DataAccessException dae) {
                meterRegistry.counter(MaterializerMetrics.DB_ERRORS).increment();
                LOG.warn("PG error processing record key={} offset={} — retrying in {}s",
                        keyString(record), record.offset(), RETRY_BACKOFF.toSeconds(), dae);
                sleep(RETRY_BACKOFF);
            }
        }
    }

    private void handle(ConsumerRecord<byte[], byte[]> record) {
        String reductionKey = keyString(record);
        if (record.value() == null) {
            deleter.delete(reductionKey);
            deleteCounter.increment();
            return;
        }
        AlarmState alarm;
        try {
            alarm = AlarmState.parseFrom(record.value());
        } catch (Exception e) {
            LOG.warn("Unparseable AlarmState key={} offset={} — skipping", reductionKey, record.offset(), e);
            skipCounter.increment();
            return;
        }
        upserter.upsert(alarm);
        upsertCounter.increment();
    }

    private static String keyString(ConsumerRecord<byte[], byte[]> record) {
        return record.key() == null ? "" : new String(record.key());
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
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "alarms-materializer-" + UUID.randomUUID());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(p);
    }
}
