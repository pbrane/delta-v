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
package org.deltav.gateway.kafka;

import com.google.protobuf.Timestamp;
import org.deltav.minion.grpc.v1.Heartbeat;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HeartbeatTranslatorTest {

    @Test
    void translate_keysKafkaRecordByLocationAndId() {
        Heartbeat hb = Heartbeat.newBuilder()
            .setMinionId("minion-A")
            .setLocation("loc-DC1")
            .setSentAt(Timestamp.newBuilder().setSeconds(1745625600L).build())
            .setVersion("1.2.0-rc1")
            .build();

        var record = HeartbeatTranslator.toKafkaRecord(hb, "minion-A", "loc-DC1");

        assertThat(record.topic()).isEqualTo("OpenNMS.Sink.Heartbeat");
        assertThat(record.key()).isEqualTo("loc-DC1@minion-A");
    }

    @Test
    void translate_emitsXmlPayloadCompatibleWithExistingConsumer() {
        Heartbeat hb = Heartbeat.newBuilder()
            .setMinionId("minion-A")
            .setLocation("loc-DC1")
            .setSentAt(Timestamp.newBuilder().setSeconds(1745625600L).build())
            .setVersion("1.2.0-rc1")
            .build();

        var record = HeartbeatTranslator.toKafkaRecord(hb, "minion-A", "loc-DC1");

        String payload = new String(record.value());
        assertThat(payload).contains("<MinionIdentityDTO>");
        assertThat(payload).contains("<id>minion-A</id>");
        assertThat(payload).contains("<location>loc-DC1</location>");
        assertThat(payload).contains("<version>1.2.0-rc1</version>");
        // 1745625600 epoch seconds = 2025-04-26T00:00:00Z
        assertThat(payload).contains("<timestamp>2025-04-26T00:00:00");
    }

    @Test
    void translate_metadataIdentityWinsOverPayloadIdentity() {
        Heartbeat hb = Heartbeat.newBuilder()
            .setMinionId("payload-id")
            .setLocation("payload-loc")
            .setSentAt(Timestamp.newBuilder().setSeconds(1745625600L).build())
            .setVersion("1.2.0-rc1")
            .build();

        var record = HeartbeatTranslator.toKafkaRecord(hb, "metadata-id", "metadata-loc");

        assertThat(record.key()).isEqualTo("metadata-loc@metadata-id");
        assertThat(new String(record.value())).contains("<id>payload-id</id>");
    }
}
