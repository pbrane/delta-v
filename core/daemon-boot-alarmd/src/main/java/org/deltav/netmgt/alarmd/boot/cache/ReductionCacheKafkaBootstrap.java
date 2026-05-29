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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.deltav.alarms.proto.AlarmState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Bootstrap-replay loader for {@link ReductionCache}. On
 * {@link ApplicationReadyEvent}, opens a raw KafkaConsumer with a random group
 * id, subscribes to {@code deltav-alarms-state-change}, seeks to the beginning,
 * and replays to the per-partition high-water-mark. After bootstrap, it remains
 * running and live-tails the topic so alarmd's own publishes flow back into the
 * cache.
 */
@Component
@ConditionalOnProperty(prefix = "deltav.alarmd.persistence", name = "cache-enabled",
        havingValue = "true", matchIfMissing = true)
public class ReductionCacheKafkaBootstrap implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReductionCacheKafkaBootstrap.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration EMPTY_POLL_THRESHOLD = Duration.ofSeconds(5);

    private final ReductionCache cache;
    private final ApplicationEventPublisher publisher;
    private final String topic;
    private final String bootstrapServers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread thread;

    public ReductionCacheKafkaBootstrap(ReductionCache cache,
                                        ApplicationEventPublisher publisher,
                                        @Value("${deltav.alarmd.kafka-publisher.topic:deltav-alarms-state-change}") String topic,
                                        @Value("${deltav.alarmd.kafka-publisher.bootstrap-servers:${spring.kafka.bootstrap-servers:}}") String bootstrapServers) {
        this.cache = cache;
        this.publisher = publisher;
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
    }

    @EventListener
    public void onReady(ApplicationReadyEvent event) {
        if (running.compareAndSet(false, true)) {
            thread = new Thread(this, "reduction-cache-bootstrap");
            thread.setDaemon(true);
            thread.start();
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (thread != null) {
            try { thread.join(5000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }

    @Override
    public void run() {
        try (KafkaConsumer<byte[], byte[]> consumer = buildConsumer()) {
            consumer.subscribe(List.of(topic));
            boolean seeked = false;
            boolean readyEmitted = false;
            Map<TopicPartition, Long> hwm = new HashMap<>();
            long lastNonEmptyMillis = System.currentTimeMillis();

            while (running.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
                if (!seeked && !consumer.assignment().isEmpty()) {
                    consumer.seekToBeginning(consumer.assignment());
                    hwm.putAll(consumer.endOffsets(consumer.assignment()));
                    seeked = true;
                    continue;
                }
                if (!records.isEmpty()) {
                    lastNonEmptyMillis = System.currentTimeMillis();
                    for (ConsumerRecord<byte[], byte[]> r : records) {
                        applyRecord(r);
                    }
                }
                if (!readyEmitted && seeked && atHighWaterMark(consumer, hwm, lastNonEmptyMillis)) {
                    cache.markReady();
                    publisher.publishEvent(new ReductionCacheReadyEvent(this, cache.size()));
                    LOG.info("ReductionCache bootstrap complete (size={})", cache.size());
                    readyEmitted = true;
                }
            }
        } catch (Exception e) {
            LOG.error("ReductionCacheKafkaBootstrap fatal error", e);
        }
    }

    private boolean atHighWaterMark(KafkaConsumer<byte[], byte[]> consumer,
                                    Map<TopicPartition, Long> hwm,
                                    long lastNonEmptyMillis) {
        for (TopicPartition tp : consumer.assignment()) {
            long pos = consumer.position(tp);
            Long end = hwm.get(tp);
            if (end != null && pos < end) {
                return false;
            }
        }
        return System.currentTimeMillis() - lastNonEmptyMillis > EMPTY_POLL_THRESHOLD.toMillis();
    }

    private void applyRecord(ConsumerRecord<byte[], byte[]> record) {
        String rk = record.key() == null ? "" : new String(record.key());
        if (rk.isEmpty()) return;
        if (record.value() == null) {
            cache.applyTombstone(rk);
            return;
        }
        try {
            AlarmState alarm = AlarmState.parseFrom(record.value());
            cache.applyUpdate(rk, alarm);
        } catch (Exception e) {
            LOG.warn("Unparseable AlarmState in ReductionCache bootstrap rk={} offset={} — skipping",
                    rk, record.offset(), e);
        }
    }

    private KafkaConsumer<byte[], byte[]> buildConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "alarmd-reduction-cache-" + UUID.randomUUID());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(p);
    }
}
