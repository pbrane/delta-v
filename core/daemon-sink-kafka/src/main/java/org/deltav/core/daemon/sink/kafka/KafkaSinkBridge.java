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
package org.deltav.core.daemon.sink.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opennms.core.ipc.sink.api.Message;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.ipc.sink.model.SinkMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Bridges Kafka Sink topic consumption to the local {@link LocalMessageConsumerManager}.
 *
 * <p>minion-gateway publishes Minion-forwarded messages (traps, syslogs, telemetry)
 * to Kafka Sink topics ({@code DeltaV.Sink.{moduleId}}). This bridge consumes from
 * that topic and dispatches to the local consumer manager.</p>
 *
 * <p>Reusable by any daemon that consumes from Minion Sink topics.</p>
 */
public class KafkaSinkBridge implements InitializingBean, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkBridge.class);
    private static final Duration POLL_DURATION = Duration.ofMillis(100);

    private final LocalMessageConsumerManager consumerManager;
    private final String bootstrapServers;
    private final String groupId;

    private volatile SinkModule<?, Message> module;
    private volatile Thread consumerThread;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaSinkBridge(LocalMessageConsumerManager consumerManager,
                           String bootstrapServers,
                           String groupId) {
        this.consumerManager = consumerManager;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    public void setModule(SinkModule<?, Message> module) {
        if (this.module != null) {
            throw new IllegalStateException(
                "KafkaSinkBridge already bound to module " + this.module.getId()
                + "; cannot rebind to " + module.getId());
        }
        this.module = module;
    }

    @Override
    public void afterPropertiesSet() {
        consumerThread = new Thread(this::pollLoop, "kafka-sink-bridge");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private void pollLoop() {
        while (module == null && !closed.get()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (closed.get()) return;

        final String topic = "DeltaV.Sink." + module.getId();
        LOG.info("KafkaSinkBridge starting: topic={}, bootstrapServers={}, groupId={}",
                topic, bootstrapServers, groupId);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (!closed.get()) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_DURATION);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            SinkMessage sinkMessage = SinkMessage.parseFrom(record.value());
                            byte[] content = sinkMessage.getContent().toByteArray();
                            Message message = module.unmarshal(content);
                            consumerManager.dispatch(module, message);
                        } catch (Exception e) {
                            LOG.warn("Error processing Sink message (offset={}): {}",
                                    record.offset(), e.getMessage(), e);
                        }
                    }
                } catch (Throwable t) {
                    if (closed.get()) break;
                    LOG.error("Error in KafkaSinkBridge poll loop: {}", t.getMessage(), t);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                LOG.error("Fatal error in KafkaSinkBridge: {}", t.getMessage(), t);
            }
        }
        LOG.info("KafkaSinkBridge stopped");
    }

    @Override
    public void destroy() {
        closed.set(true);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
