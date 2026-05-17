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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsSeverity;

class KafkaAlarmPublisherTest {

    private static final String TOPIC = "deltav-alarms-state-change";

    /**
     * Kafka 4.1.1 removed the 3-arg MockProducer(boolean, Serializer, Serializer) constructor.
     * The nearest equivalent is the 4-arg (boolean, Partitioner, Serializer, Serializer);
     * passing null for Partitioner uses the default round-robin behaviour.
     */
    private static MockProducer<String, byte[]> newMockProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    }

    private static OnmsAlarm sampleAlarm() {
        OnmsAlarm a = new OnmsAlarm();
        a.setId(7);
        a.setReductionKey("uei.opennms.org/nodes/nodeDown::7");
        a.setUei("uei.opennms.org/nodes/nodeDown");
        a.setSeverity(OnmsSeverity.MAJOR);
        return a;
    }

    @Test
    void publishesProtoOnNewOrUpdatedAlarm() throws Exception {
        MockProducer<String, byte[]> producer = newMockProducer();
        KafkaAlarmPublisher publisher = new KafkaAlarmPublisher(producer, TOPIC, new AlarmStateMapper());

        publisher.handleNewOrUpdatedAlarm(sampleAlarm());

        List<ProducerRecord<String, byte[]>> history = producer.history();
        assertThat(history).hasSize(1);
        assertThat(history.get(0).topic()).isEqualTo(TOPIC);
        assertThat(history.get(0).key()).isEqualTo("uei.opennms.org/nodes/nodeDown::7");
        AlarmState decoded = AlarmState.parseFrom(history.get(0).value());
        assertThat(decoded.getAlarmId()).isEqualTo(7);
        assertThat(decoded.getSeverity()).isEqualTo(AlarmState.Severity.MAJOR);
    }

    @Test
    void publishesTombstoneOnDeletedAlarm() {
        MockProducer<String, byte[]> producer = newMockProducer();
        KafkaAlarmPublisher publisher = new KafkaAlarmPublisher(producer, TOPIC, new AlarmStateMapper());

        publisher.handleDeletedAlarm(7, "uei.opennms.org/nodes/nodeDown::7");

        List<ProducerRecord<String, byte[]>> history = producer.history();
        assertThat(history).hasSize(1);
        assertThat(history.get(0).key()).isEqualTo("uei.opennms.org/nodes/nodeDown::7");
        assertThat(history.get(0).value()).isNull(); // tombstone for log compaction
    }

    @Test
    void snapshotCallbacksDoNotPublish() {
        MockProducer<String, byte[]> producer = newMockProducer();
        KafkaAlarmPublisher publisher = new KafkaAlarmPublisher(producer, TOPIC, new AlarmStateMapper());

        publisher.preHandleAlarmSnapshot();
        publisher.handleAlarmSnapshot(Collections.emptyList());
        publisher.postHandleAlarmSnapshot();

        assertThat(producer.history()).isEmpty();
    }
}
