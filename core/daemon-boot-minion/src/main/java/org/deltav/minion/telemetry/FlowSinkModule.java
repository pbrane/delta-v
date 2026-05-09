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
package org.deltav.minion.telemetry;

import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;

import org.deltav.minion.telemetry.proto.TelemetryProtos.TelemetryMessageLog;
import org.opennms.core.ipc.sink.api.AggregationPolicy;
import org.opennms.core.ipc.sink.api.AsyncPolicy;
import org.opennms.core.ipc.sink.api.SinkModule;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Sink module that ships per-datagram {@link FlowTelemetryMessage}
 * envelopes to Kafka topic {@code DeltaV.Sink.Telemetry-{protocol}}.
 *
 * <p>Each UDP datagram received by {@code FlowUdpListener} becomes one
 * Kafka message. There is no aggregation in this first cut — the
 * {@link AggregationPolicy} returns {@code null}, meaning S and T are
 * the same type and the Sink dispatcher serializes each message
 * individually. Aggregation can be added later as an optimization
 * matching horizon's {@code TelemetrySinkModule}, which batches up to
 * 1000 messages per 500ms keyed by exporter address.
 *
 * <p>{@code getNumConsumerThreads()} returns {@code 0} because the
 * Minion is a producer for these topics; the consumer thread count is
 * only consulted on the consumer side (flow-enricher / Telemetryd).
 */
public class FlowSinkModule implements SinkModule<FlowTelemetryMessage, FlowTelemetryMessage> {

    private final FlowProtocol protocol;
    private final int queueSize;
    private final int numThreads;

    public FlowSinkModule(FlowProtocol protocol, int queueSize, int numThreads) {
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be positive, got " + queueSize);
        }
        if (numThreads <= 0) {
            throw new IllegalArgumentException("numThreads must be positive, got " + numThreads);
        }
        this.queueSize = queueSize;
        this.numThreads = numThreads;
    }

    @Override
    public String getId() {
        return protocol.getSinkModuleId();
    }

    @Override
    public int getNumConsumerThreads() {
        return 0;
    }

    @Override
    public byte[] marshal(FlowTelemetryMessage message) {
        return message.toByteArray();
    }

    @Override
    public FlowTelemetryMessage unmarshal(byte[] bytes) {
        try {
            return new FlowTelemetryMessage(TelemetryMessageLog.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedIOException("Failed to parse TelemetryMessageLog", e);
        }
    }

    @Override
    public byte[] marshalSingleMessage(FlowTelemetryMessage message) {
        return marshal(message);
    }

    @Override
    public FlowTelemetryMessage unmarshalSingleMessage(byte[] message) {
        return unmarshal(message);
    }

    @Override
    public AggregationPolicy<FlowTelemetryMessage, FlowTelemetryMessage, ?> getAggregationPolicy() {
        return null;
    }

    @Override
    public AsyncPolicy getAsyncPolicy() {
        return new AsyncPolicy() {
            @Override
            public int getQueueSize() {
                return queueSize;
            }

            @Override
            public int getNumThreads() {
                return numThreads;
            }

            @Override
            public boolean isBlockWhenFull() {
                return false;
            }
        };
    }

    /**
     * Returns a routing key composed of {@code location@sourceAddress:sourcePort}
     * so the Sink API's Kafka producer partitions messages by exporter.
     *
     * <p>This matches horizon's {@code TelemetrySinkModule.getRoutingKey} pattern
     * and is important for Phase 2 (server-side parsing in the flow-enricher):
     * Netflow v9 and IPFIX parsers maintain per-exporter template caches, so
     * all packets from a single exporter must consistently land on the same
     * consumer instance to keep the template cache warm. Round-robin partitioning
     * (the default when this method returns {@code Optional.empty()}) would
     * cause cold-cache storms every time a Kafka rebalance moved an exporter's
     * session to a different flow-enricher replica.
     *
     * <p>Even though the Minion itself does no template caching in the current
     * thin-relay design, setting the routing key here means Phase 2 can ship
     * without needing to come back and fix Minion-side partitioning.
     */
    @Override
    public Optional<String> getRoutingKey(FlowTelemetryMessage message) {
        var log = message.getTelemetryMessageLog();
        return Optional.of(String.format("%s@%s:%d",
                log.getLocation(),
                log.getSourceAddress(),
                log.getSourcePort()));
    }
}
