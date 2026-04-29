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
import org.deltav.minion.grpc.v1.SyslogAck;
import org.deltav.minion.grpc.v1.SyslogMessage;
import org.deltav.minion.grpc.v1.SyslogServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.time.Instant;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;

/**
 * Bidi-streaming gRPC SyslogService. Per-onNext: forwards opaque payload
 * bytes to {@code OpenNMS.Sink.Syslog} keyed by {@code <location>@<minion-id>}.
 *
 * <p>Per Decision 6 (sinks are lossy by design): Kafka publish failures
 * log and continue; the gRPC stream is not torn down on a single
 * publish failure. Identity is read from the gRPC Context (set by
 * {@code MinionIdentityServerInterceptor}, established in rc1).
 */
@GrpcService
public class SyslogGrpcService extends SyslogServiceGrpc.SyslogServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(SyslogGrpcService.class);
    private static final String TOPIC = "OpenNMS.Sink.Syslog";

    private final SinkKafkaProducer producer;

    public SyslogGrpcService(SinkKafkaProducer producer) {
        this.producer = producer;
    }

    @Override
    public StreamObserver<SyslogMessage> publish(StreamObserver<SyslogAck> ack) {
        String minionId = MINION_ID_CTX.get();
        String location = MINION_LOCATION_CTX.get();
        String key = location + "@" + minionId;
        LOG.info("Syslog stream opened for minion={} location={}", minionId, location);

        return new StreamObserver<>() {
            @Override
            public void onNext(SyslogMessage msg) {
                producer.send(TOPIC, key, msg.getPayload().toByteArray())
                    .thenAccept(v -> ack.onNext(buildAck()))
                    .exceptionally(t -> {
                        LOG.warn("Syslog publish failed for minion={}", minionId, t);
                        return null;
                    });
            }
            @Override
            public void onError(Throwable t) {
                LOG.info("Syslog stream error for minion={}: {}", minionId, t.toString());
            }
            @Override
            public void onCompleted() {
                LOG.info("Syslog stream closed for minion={}", minionId);
                ack.onCompleted();
            }
        };
    }

    private SyslogAck buildAck() {
        Instant n = Instant.now();
        return SyslogAck.newBuilder()
            .setReceivedAt(Timestamp.newBuilder().setSeconds(n.getEpochSecond()).setNanos(n.getNano()).build())
            .build();
    }
}
