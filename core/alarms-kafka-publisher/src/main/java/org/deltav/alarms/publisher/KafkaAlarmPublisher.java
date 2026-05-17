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
package org.deltav.alarms.publisher;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deltav.alarms.proto.AlarmState;
import org.opennms.netmgt.alarmd.api.AlarmLifecycleListener;
import org.opennms.netmgt.model.OnmsAlarm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes alarm lifecycle changes to the {@code deltav-alarms-state-change}
 * Kafka topic as {@link AlarmState} protobuf.
 *
 * <p>Phase 1 of the "Alarms on Kafka" migration: alarmd still writes
 * PostgreSQL directly; Kafka publication is an additional output. Records are
 * keyed by {@code reduction_key}; a deleted alarm produces a null-valued
 * tombstone so a compacted topic drops it.</p>
 *
 * <p>The periodic snapshot callbacks are intentionally no-ops — the publisher
 * is an incremental event stream, not a state sync.</p>
 */
public class KafkaAlarmPublisher implements AlarmLifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAlarmPublisher.class);

    private final Producer<String, byte[]> producer;
    private final String topic;
    private final AlarmStateMapper mapper;

    public KafkaAlarmPublisher(Producer<String, byte[]> producer, String topic, AlarmStateMapper mapper) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public void handleNewOrUpdatedAlarm(OnmsAlarm alarm) {
        if (alarm == null || alarm.getReductionKey() == null) {
            LOG.warn("Skipping alarm with null reduction key: {}", alarm);
            return;
        }
        try {
            AlarmState proto = mapper.toProto(alarm);
            producer.send(new ProducerRecord<>(topic, alarm.getReductionKey(), proto.toByteArray()));
            LOG.debug("Published alarm {} (rk={}) to {}", proto.getAlarmId(), alarm.getReductionKey(), topic);
        } catch (Exception e) {
            LOG.error("Failed to publish alarm rk={} to {}", alarm.getReductionKey(), topic, e);
        }
    }

    @Override
    public void handleDeletedAlarm(int alarmId, String reductionKey) {
        if (reductionKey == null) {
            LOG.warn("Skipping deleted alarm {} with null reduction key", alarmId);
            return;
        }
        try {
            producer.send(new ProducerRecord<>(topic, reductionKey, null)); // tombstone
            LOG.debug("Published tombstone for alarm {} (rk={}) to {}", alarmId, reductionKey, topic);
        } catch (Exception e) {
            LOG.error("Failed to publish tombstone rk={} to {}", reductionKey, topic, e);
        }
    }

    @Override
    public void preHandleAlarmSnapshot() {
        // no-op — incremental publisher, not a state sync
    }

    @Override
    public void handleAlarmSnapshot(List<OnmsAlarm> alarms) {
        // no-op — incremental publisher, not a state sync
    }

    @Override
    public void postHandleAlarmSnapshot() {
        // no-op — incremental publisher, not a state sync
    }
}
