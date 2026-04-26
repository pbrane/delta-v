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
package org.deltav.minion.common.grpc;

import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.Heartbeat;
import org.deltav.minion.grpc.v1.HeartbeatAck;
import org.deltav.minion.grpc.v1.HeartbeatServiceGrpc;
import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.ipc.sink.api.Message;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.ipc.sink.api.SyncDispatcher;
import org.opennms.distributed.core.api.MinionIdentity;
import org.opennms.minion.heartbeat.common.MinionIdentityDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * gRPC-backed {@link MessageDispatcherFactory}. rc1 implements only Heartbeat;
 * any other module-id requested throws {@link UnsupportedOperationException} so
 * a misconfigured listener can't silently fall back. Keeps the rc1 surface
 * channel-isolated per feedback_e2e_continuous_validation_grpc_migration.
 *
 * <p>Identity headers are attached at the stub level via
 * {@link MinionIdentityClientInterceptor} so every {@code Publish} call carries
 * {@code x-minion-id} / {@code x-minion-location} metadata for minion-gateway
 * routing.
 */
public class GrpcMessageDispatcherFactory implements MessageDispatcherFactory {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageDispatcherFactory.class);

    private final ManagedChannel channel;
    private final HeartbeatServiceGrpc.HeartbeatServiceStub heartbeatStub;
    private final AtomicReference<StreamObserver<Heartbeat>> heartbeatStream = new AtomicReference<>();

    public GrpcMessageDispatcherFactory(ManagedChannel channel, MinionIdentity identity) {
        this.channel = channel;
        this.heartbeatStub = HeartbeatServiceGrpc.newStub(channel)
            .withInterceptors(new MinionIdentityClientInterceptor(
                identity.getId(), identity.getLocation()));
    }

    @Override
    public <S extends Message, T extends Message> SyncDispatcher<S> createSyncDispatcher(SinkModule<S, T> module) {
        if (SinkModule.HEARTBEAT_MODULE_ID.equals(module.getId())) {
            @SuppressWarnings("unchecked")
            SyncDispatcher<S> typed = (SyncDispatcher<S>) heartbeatSyncDispatcher();
            return typed;
        }
        throw new UnsupportedOperationException(
            "GrpcMessageDispatcherFactory rc1 supports module '" + SinkModule.HEARTBEAT_MODULE_ID + "' only; "
            + "got '" + module.getId() + "'. Other sinks remain on Kafka in rc1 - "
            + "verify HeartbeatConfiguration is the only @Qualifier consumer.");
    }

    @Override
    public <S extends Message, T extends Message> AsyncDispatcher<S> createAsyncDispatcher(SinkModule<S, T> module) {
        throw new UnsupportedOperationException(
            "GrpcMessageDispatcherFactory rc1 supports sync dispatch only "
            + "(Heartbeat is sync); async sinks remain on Kafka.");
    }

    private SyncDispatcher<MinionIdentityDTO> heartbeatSyncDispatcher() {
        return new SyncDispatcher<>() {
            @Override
            public void send(MinionIdentityDTO identity) {
                StreamObserver<Heartbeat> stream = heartbeatStream.updateAndGet(s -> s != null ? s : openStream());
                Instant now = identity.getTimestamp() != null
                    ? identity.getTimestamp().toInstant() : Instant.now();
                Heartbeat hb = Heartbeat.newBuilder()
                    .setMinionId(identity.getId())
                    .setLocation(identity.getLocation())
                    .setSentAt(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano()).build())
                    .setVersion(identity.getVersion() != null ? identity.getVersion() : "")
                    .build();
                try {
                    stream.onNext(hb);
                    LOG.debug("Sent heartbeat via gRPC for minion={} location={}",
                        identity.getId(), identity.getLocation());
                } catch (RuntimeException e) {
                    LOG.warn("Heartbeat stream send failed; resetting stream", e);
                    heartbeatStream.set(null);
                    throw e;
                }
            }

            @Override
            public void close() {
                StreamObserver<Heartbeat> s = heartbeatStream.getAndSet(null);
                if (s != null) {
                    s.onCompleted();
                }
                channel.shutdown();
            }
        };
    }

    private StreamObserver<Heartbeat> openStream() {
        StreamObserver<HeartbeatAck> ackObserver = new StreamObserver<>() {
            @Override
            public void onNext(HeartbeatAck ack) {
                LOG.debug("Received ack for minion={}", ack.getMinionId());
            }
            @Override
            public void onError(Throwable t) {
                LOG.warn("Heartbeat ack stream error; channel will reconnect on next send", t);
                heartbeatStream.set(null);
            }
            @Override
            public void onCompleted() {
                heartbeatStream.set(null);
            }
        };
        return heartbeatStub.publish(ackObserver);
    }
}
