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
package org.deltav.gateway.sink;

import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.TelemetryAck;
import org.deltav.minion.grpc.v1.TelemetryDatagram;
import org.deltav.minion.grpc.v1.TelemetryServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.time.Instant;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;

/**
 * Bidi-streaming gRPC TelemetryService. Four protocol-specific {@code publish*}
 * methods route opaque payload bytes to {@code DeltaV.Sink.Telemetry-<proto>}
 * topics keyed by {@code <location>@<minion-id>}. Routing is by RPC method,
 * not by inspecting payload — the Minion picks the topic by calling the
 * matching method on the generated stub.
 *
 * <p>Per Decision 6 (sinks are lossy by design): Kafka publish failures
 * log and continue; the gRPC stream is not torn down on a single
 * publish failure. Identity is read from the gRPC Context (set by
 * {@code MinionIdentityServerInterceptor}, established in rc1).
 */
@GrpcService
public class TelemetryGrpcService extends TelemetryServiceGrpc.TelemetryServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryGrpcService.class);
    private static final String IPFIX_TOPIC = "DeltaV.Sink.Telemetry-IPFIX";
    private static final String NETFLOW5_TOPIC = "DeltaV.Sink.Telemetry-Netflow-5";
    private static final String NETFLOW9_TOPIC = "DeltaV.Sink.Telemetry-Netflow-9";
    private static final String SFLOW_TOPIC = "DeltaV.Sink.Telemetry-SFlow";

    private final SinkKafkaProducer producer;

    public TelemetryGrpcService(SinkKafkaProducer producer) {
        this.producer = producer;
    }

    @Override
    public StreamObserver<TelemetryDatagram> publishIpfix(StreamObserver<TelemetryAck> responseObserver) {
        return publishTo(IPFIX_TOPIC, responseObserver);
    }

    @Override
    public StreamObserver<TelemetryDatagram> publishNetflow5(StreamObserver<TelemetryAck> responseObserver) {
        return publishTo(NETFLOW5_TOPIC, responseObserver);
    }

    @Override
    public StreamObserver<TelemetryDatagram> publishNetflow9(StreamObserver<TelemetryAck> responseObserver) {
        return publishTo(NETFLOW9_TOPIC, responseObserver);
    }

    @Override
    public StreamObserver<TelemetryDatagram> publishSflow(StreamObserver<TelemetryAck> responseObserver) {
        return publishTo(SFLOW_TOPIC, responseObserver);
    }

    /**
     * Common bidi handler shared by all four protocol-specific overrides.
     * Each onNext forwards opaque {@code TelemetryDatagram.payload} bytes
     * to the protocol-specific Kafka topic. Per Decision 6 (lossy by design):
     * Kafka publish failures log and continue; the gRPC stream survives a
     * single publish failure.
     */
    private StreamObserver<TelemetryDatagram> publishTo(String topic, StreamObserver<TelemetryAck> responseObserver) {
        String minionId = MINION_ID_CTX.get();
        String location = MINION_LOCATION_CTX.get();
        String key = location + "@" + minionId;
        LOG.info("Telemetry stream opened for minion={} location={} topic={}", minionId, location, topic);

        return new StreamObserver<>() {
            @Override
            public void onNext(TelemetryDatagram msg) {
                producer.send(topic, key, msg.getPayload().toByteArray())
                    .thenAccept(v -> responseObserver.onNext(buildAck()))
                    .exceptionally(t -> {
                        LOG.warn("Telemetry publish failed for minion={} topic={}", minionId, topic, t);
                        return null;
                    });
            }

            @Override
            public void onError(Throwable t) {
                LOG.info("Telemetry stream error for minion={} topic={}: {}", minionId, topic, t.toString());
            }

            @Override
            public void onCompleted() {
                LOG.info("Telemetry stream closed for minion={} topic={}", minionId, topic);
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Build the per-message ack. Called on the Kafka producer's I/O thread
     * (via the {@code thenAccept} callback), not the gRPC stream thread —
     * the timestamp is "ack issued at," not "Minion-emitted at."
     */
    private TelemetryAck buildAck() {
        Instant now = Instant.now();
        return TelemetryAck.newBuilder()
            .setReceivedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build())
            .build();
    }
}
