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

import org.opennms.netmgt.telemetry.common.ipc.TelemetryProtos;

/**
 * Result of deserializing a Kafka Sink topic record. Carries both the Sink
 * module identifier (used for protocol dispatch in
 * {@link FlowEnrichmentFunction}) and the decoded
 * {@link TelemetryProtos.TelemetryMessageLog} payload (passed to the protocol
 * adapter).
 *
 * <p><strong>Note on moduleId source:</strong> the {@code SinkMessage}
 * protobuf envelope on the Kafka wire (class
 * {@code org.opennms.core.ipc.sink.model.SinkMessage}) does <em>not</em>
 * carry a {@code moduleId} field &mdash; its fields are {@code messageId},
 * {@code content}, {@code currentChunkNumber}, {@code totalChunks}, and
 * {@code tracingInfo}. The Sink IPC framework derives the module identifier
 * from the Kafka topic name ({@code DeltaV.Sink.{moduleId}}; see
 * {@code KafkaSinkBridge} in {@code core/daemon-sink-kafka}).
 *
 * <p>The single-argument {@link SinkMessageDeserializer#deserialize(byte[])}
 * overload therefore populates {@code moduleId} as {@code null}. Callers that
 * know the topic name (e.g. the Spring Cloud Stream binding layer) should
 * use the two-argument
 * {@link SinkMessageDeserializer#deserialize(String, byte[])} overload to
 * supply the moduleId explicitly. Wiring topic headers through the Spring
 * Cloud Stream function signature happens in a later commit of the Phase 1.5
 * effort.
 *
 * @param moduleId the Sink module identifier (e.g. {@code "Telemetry-Netflow-5"}),
 *                 or {@code null} when the deserializer cannot determine it
 *                 from the payload alone
 * @param messageLog the decoded telemetry payload; must not be {@code null}
 */
public record DeserializedSinkMessage(String moduleId, TelemetryProtos.TelemetryMessageLog messageLog) {

    /**
     * Compact constructor enforcing a non-null {@code messageLog}. The
     * deserializer returns {@code null} in place of this record when the
     * payload is unparseable, so callers can rely on any non-null
     * {@code DeserializedSinkMessage} carrying a usable message log.
     */
    public DeserializedSinkMessage {
        java.util.Objects.requireNonNull(messageLog, "messageLog");
    }
}
