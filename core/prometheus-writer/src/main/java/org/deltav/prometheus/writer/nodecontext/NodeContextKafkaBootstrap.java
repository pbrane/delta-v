/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodecontext;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.deltav.prometheus.writer.metrics.PrometheusWriterMetrics;
import org.deltav.timeseries.proto.NodeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dedicated Kafka consumer for {@code deltav-node-context}. On startup, records the
 * end-offset high-water mark (HWM) per partition, seeks to earliest, consumes
 * records until every partition's offset catches the recorded HWM, and marks
 * the cache ready. Then transitions to a live-tail loop.
 */
@Component
public class NodeContextKafkaBootstrap implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeContextKafkaBootstrap.class);
    private static final String TOPIC = "deltav-node-context";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private final NodeContextCache cache;
    private final ApplicationEventPublisher eventPublisher;
    private final MeterRegistry meterRegistry;
    private final String bootstrapServers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread thread;
    private Timer.Sample bootstrapSample;

    public NodeContextKafkaBootstrap(
            NodeContextCache cache,
            ApplicationEventPublisher eventPublisher,
            MeterRegistry meterRegistry,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.cache = cache;
        this.eventPublisher = eventPublisher;
        this.meterRegistry = meterRegistry;
        this.bootstrapServers = bootstrapServers;
    }

    @EventListener
    public void onAppReady(ApplicationReadyEvent event) {
        start();
    }

    public void start() {
        running.set(true);
        bootstrapSample = Timer.start(meterRegistry);
        thread = new Thread(this, "node-context-bootstrap");
        thread.setDaemon(true);
        thread.start();
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
        try (KafkaConsumer<String, byte[]> consumer = buildConsumer()) {
            List<PartitionInfo> parts = consumer.partitionsFor(TOPIC);
            if (parts == null || parts.isEmpty()) {
                LOG.warn("Topic {} has no partitions yet — marking cache ready with empty state", TOPIC);
                finishBootstrap();
                liveTail(consumer);
                return;
            }
            Set<TopicPartition> allParts = new HashSet<>();
            for (PartitionInfo p : parts) {
                allParts.add(new TopicPartition(TOPIC, p.partition()));
            }
            consumer.assign(allParts);

            Map<TopicPartition, Long> endOffsets = new HashMap<>(consumer.endOffsets(allParts));
            consumer.seekToBeginning(allParts);

            Set<TopicPartition> caughtUp = new HashSet<>();
            // An empty partition (end offset == 0) is instantly "caught up".
            for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
                if (e.getValue() == 0L) {
                    caughtUp.add(e.getKey());
                }
            }

            while (running.get() && caughtUp.size() < allParts.size()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, byte[]> r : records) {
                    applyRecord(r);
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    if (r.offset() + 1 >= endOffsets.get(tp)) {
                        caughtUp.add(tp);
                    }
                }
            }
            finishBootstrap();
            liveTail(consumer);
        } catch (Exception e) {
            LOG.error("NodeContextKafkaBootstrap fatal error", e);
        }
    }

    private void liveTail(KafkaConsumer<String, byte[]> consumer) {
        while (running.get()) {
            try {
                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, byte[]> r : records) {
                    applyRecord(r);
                }
            } catch (Exception e) {
                LOG.warn("NodeContextKafkaBootstrap live-tail error — will retry after poll timeout", e);
            }
        }
    }

    private void applyRecord(ConsumerRecord<String, byte[]> r) {
        if (r.value() == null) {
            // Kafka log-compaction tombstone (null value). Treat as delete.
            cache.remove(r.key());
            return;
        }
        try {
            NodeContext nc = NodeContext.parseFrom(r.value());
            cache.applyUpdate(r.key(), nc);
        } catch (Exception e) {
            LOG.warn("Malformed NodeContext record key={} offset={} — skipping", r.key(), r.offset(), e);
        }
    }

    private void finishBootstrap() {
        cache.markReady();
        long durationNs = bootstrapSample.stop(
                meterRegistry.timer(PrometheusWriterMetrics.NC_BOOTSTRAP_DURATION));
        long durationMillis = durationNs / 1_000_000L;
        LOG.info("NodeContextCache bootstrap complete: {} entries in {} ms", cache.size(), durationMillis);
        eventPublisher.publishEvent(new NodeContextCacheReadyEvent(this, durationMillis, cache.size()));
    }

    private KafkaConsumer<String, byte[]> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "prometheus-writer-node-context-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // we manage offsets via seek
        return new KafkaConsumer<>(props);
    }
}
