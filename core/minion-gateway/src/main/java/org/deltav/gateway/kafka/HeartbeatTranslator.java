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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deltav.minion.grpc.v1.Heartbeat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Pure translator from gRPC {@link Heartbeat} to a Kafka {@link ProducerRecord}
 * carrying horizon's MinionIdentityDTO XML payload. Identity passed in as
 * separate parameters is the authoritative routing source — payload identity
 * is duplicate-for-logging only.
 */
public final class HeartbeatTranslator {

    public static final String TOPIC = "OpenNMS.Sink.Heartbeat";

    private static final DateTimeFormatter ISO =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC);

    private HeartbeatTranslator() {}

    public static ProducerRecord<String, byte[]> toKafkaRecord(
            Heartbeat hb, String minionId, String location) {
        String key = location + "@" + minionId;
        String xml = renderXml(hb);
        return new ProducerRecord<>(TOPIC, key, xml.getBytes(StandardCharsets.UTF_8));
    }

    private static String renderXml(Heartbeat hb) {
        Instant sent = toInstant(hb.getSentAt());
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
            + "<MinionIdentityDTO>"
            + "<id>" + escape(hb.getMinionId()) + "</id>"
            + "<location>" + escape(hb.getLocation()) + "</location>"
            + "<timestamp>" + ISO.format(sent) + "</timestamp>"
            + "<version>" + escape(hb.getVersion()) + "</version>"
            + "</MinionIdentityDTO>";
    }

    private static Instant toInstant(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
