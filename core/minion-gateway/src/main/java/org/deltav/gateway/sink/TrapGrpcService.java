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
import org.deltav.minion.grpc.v1.SnmpTrap;
import org.deltav.minion.grpc.v1.TrapAck;
import org.deltav.minion.grpc.v1.TrapServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.time.Instant;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;

/**
 * Bidi-streaming gRPC TrapService. Per-onNext: forwards opaque payload
 * bytes to {@code OpenNMS.Sink.Trap} keyed by {@code <location>@<minion-id>}.
 *
 * <p>Per Decision 6 (sinks are lossy by design): Kafka publish failures
 * log and continue; the gRPC stream is not torn down on a single
 * publish failure. Identity is read from the gRPC Context (set by
 * {@code MinionIdentityServerInterceptor}, established in rc1).
 */
@GrpcService
public class TrapGrpcService extends TrapServiceGrpc.TrapServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TrapGrpcService.class);
    private static final String TOPIC = "OpenNMS.Sink.Trap";

    private final SinkKafkaProducer producer;

    public TrapGrpcService(SinkKafkaProducer producer) {
        this.producer = producer;
    }

    @Override
    public StreamObserver<SnmpTrap> publish(StreamObserver<TrapAck> responseObserver) {
        String minionId = MINION_ID_CTX.get();
        String location = MINION_LOCATION_CTX.get();
        String key = location + "@" + minionId;
        LOG.info("Trap stream opened for minion={} location={}", minionId, location);

        return new StreamObserver<>() {
            @Override
            public void onNext(SnmpTrap msg) {
                producer.send(TOPIC, key, msg.getPayload().toByteArray())
                    .thenAccept(v -> responseObserver.onNext(buildAck()))
                    .exceptionally(t -> {
                        LOG.warn("Trap publish failed for minion={}", minionId, t);
                        return null;
                    });
            }

            @Override
            public void onError(Throwable t) {
                LOG.info("Trap stream error for minion={}: {}", minionId, t.toString());
            }

            @Override
            public void onCompleted() {
                LOG.info("Trap stream closed for minion={}", minionId);
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Build the per-message ack. Called on the Kafka producer's I/O thread
     * (via the {@code thenAccept} callback), not the gRPC stream thread —
     * the timestamp is "ack issued at," not "Minion-emitted at."
     */
    private TrapAck buildAck() {
        Instant now = Instant.now();
        return TrapAck.newBuilder()
            .setReceivedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build())
            .build();
    }
}
