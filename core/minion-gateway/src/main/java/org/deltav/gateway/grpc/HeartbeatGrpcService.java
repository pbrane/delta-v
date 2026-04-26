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
package org.deltav.gateway.grpc;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;

import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import org.deltav.gateway.kafka.HeartbeatKafkaProducer;
import org.deltav.gateway.kafka.HeartbeatTranslator;
import org.deltav.minion.grpc.v1.Heartbeat;
import org.deltav.minion.grpc.v1.HeartbeatAck;
import org.deltav.minion.grpc.v1.HeartbeatServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.time.Instant;

/**
 * Bidi-streaming gRPC HeartbeatService. Identity is read from the gRPC
 * Context (set by {@link MinionIdentityServerInterceptor}); the translator
 * uses that identity for the Kafka record key, while the Heartbeat
 * payload identity is preserved in the XML body for compatibility with
 * the existing horizon consumer.
 *
 * <p>Per {@code feedback_rpc_timeout_no_outages} and Phase 0 Decision 3:
 * Kafka publish failures must not crash the gRPC stream. The
 * {@code exceptionally} branch logs and continues so the gRPC client
 * can keep streaming; transient broker outages recover at the producer
 * layer without surfacing to the Minion.
 */
@GrpcService
public class HeartbeatGrpcService extends HeartbeatServiceGrpc.HeartbeatServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatGrpcService.class);

    private final HeartbeatKafkaProducer kafkaProducer;

    public HeartbeatGrpcService(HeartbeatKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public StreamObserver<Heartbeat> publish(StreamObserver<HeartbeatAck> responseObserver) {
        String minionId = MINION_ID_CTX.get();
        String location = MINION_LOCATION_CTX.get();
        LOG.info("Heartbeat stream opened for minion={} location={}", minionId, location);

        return new StreamObserver<>() {
            @Override
            public void onNext(Heartbeat hb) {
                kafkaProducer.send(HeartbeatTranslator.toKafkaRecord(hb, minionId, location))
                    .thenAccept(v -> responseObserver.onNext(buildAck(minionId)))
                    .exceptionally(t -> {
                        LOG.warn("Heartbeat publish failed for minion={}", minionId, t);
                        return null;
                    });
            }

            @Override
            public void onError(Throwable t) {
                LOG.info("Heartbeat stream error for minion={}: {}", minionId, t.toString());
            }

            @Override
            public void onCompleted() {
                LOG.info("Heartbeat stream closed for minion={}", minionId);
                responseObserver.onCompleted();
            }
        };
    }

    private HeartbeatAck buildAck(String minionId) {
        Instant now = Instant.now();
        return HeartbeatAck.newBuilder()
            .setMinionId(minionId)
            .setReceivedAt(Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano()).build())
            .build();
    }
}
