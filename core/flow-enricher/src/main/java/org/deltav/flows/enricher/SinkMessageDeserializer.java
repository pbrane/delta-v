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
package org.deltav.flows.enricher;

import org.opennms.core.ipc.sink.model.SinkMessage;
import org.opennms.netmgt.telemetry.common.ipc.TelemetryProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unwraps the two-layer protobuf encoding used on Kafka Sink topics:
 * Kafka bytes &rarr; {@link SinkMessage} envelope &rarr;
 * {@link TelemetryProtos.TelemetryMessageLog}.
 *
 * <p>The Sink IPC framework (OpenNMS Minion) supports message chunking for
 * payloads that exceed the Kafka message size limit. Flow telemetry messages
 * are typically small (single UDP packet payload) so this deserializer takes
 * the simpler path of dropping any chunked message it encounters and logging
 * a warning. Reassembly across chunks can be added later if it proves
 * necessary in production.
 *
 * <p><strong>moduleId source:</strong> the {@link SinkMessage} envelope does
 * not carry a {@code moduleId} field; in the Sink IPC protocol the module
 * identifier is derived from the Kafka topic name
 * ({@code DeltaV.Sink.{moduleId}}). Callers that know the topic should use
 * the {@link #deserialize(String, byte[])} overload to supply the moduleId
 * explicitly. The {@link #deserialize(byte[])} overload populates the
 * {@link DeserializedSinkMessage#moduleId() moduleId} field as {@code null}.
 */
public class SinkMessageDeserializer {

    private static final Logger LOG = LoggerFactory.getLogger(SinkMessageDeserializer.class);

    /**
     * Deserializes a Kafka payload into a {@link DeserializedSinkMessage}
     * with an unknown (null) moduleId. Returns {@code null} if the payload is
     * null, malformed, or chunked. Callers should treat null as
     * "drop and continue".
     *
     * @deprecated Use {@link #deserialize(String, byte[])} instead. The
     *     single-arg overload returns a {@link DeserializedSinkMessage} with
     *     {@code moduleId == null}, which causes
     *     {@link FlowEnrichmentFunction} to drop the message because protocol
     *     dispatch requires a known module ID. This overload exists only for
     *     backward compatibility with test code that doesn't care about
     *     moduleId.
     */
    @Deprecated
    public DeserializedSinkMessage deserialize(byte[] kafkaBytes) {
        return deserialize(null, kafkaBytes);
    }

    /**
     * Deserializes a Kafka payload into a {@link DeserializedSinkMessage},
     * tagging the result with the given moduleId (typically derived from the
     * Kafka topic name by the caller). Returns {@code null} if the payload is
     * null, malformed, or chunked.
     */
    public DeserializedSinkMessage deserialize(String moduleId, byte[] kafkaBytes) {
        if (kafkaBytes == null) {
            return null;
        }
        try {
            SinkMessage sinkMessage = SinkMessage.parseFrom(kafkaBytes);
            if (sinkMessage.getTotalChunks() > 1) {
                LOG.warn("Chunked SinkMessage not supported (messageId={}, totalChunks={}); dropping",
                        sinkMessage.getMessageId(), sinkMessage.getTotalChunks());
                return null;
            }
            TelemetryProtos.TelemetryMessageLog log =
                    TelemetryProtos.TelemetryMessageLog.parseFrom(sinkMessage.getContent());
            return new DeserializedSinkMessage(moduleId, log);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize SinkMessage payload ({} bytes): {}",
                    kafkaBytes.length, e.getMessage());
            return null;
        }
    }
}
