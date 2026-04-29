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

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.SnmpTrap;
import org.deltav.minion.grpc.v1.SyslogMessage;
import org.deltav.minion.grpc.v1.SyslogServiceGrpc;
import org.deltav.minion.grpc.v1.TelemetryAck;
import org.deltav.minion.grpc.v1.TelemetryDatagram;
import org.deltav.minion.grpc.v1.TelemetryServiceGrpc;
import org.deltav.minion.grpc.v1.TrapServiceGrpc;
import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.ipc.sink.api.Message;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.ipc.sink.api.SyncDispatcher;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * gRPC-backed {@link MessageDispatcherFactory} for Minion sinks.
 * Routes by {@link SinkModule#getId() module id}:
 * Syslog / Trap / Telemetry-{IPFIX,Netflow-5,Netflow-9,SFlow}.
 *
 * <p>Each module gets a single long-lived client-streaming gRPC stream
 * cached in {@link SinkStreamRegistry}. On stream error, the entry is
 * reset and the next send opens a fresh stream.
 *
 * <p>Per Decision 4 sub-decision 4-i, this factory exists alongside the
 * Kafka factories — they're mutually exclusive on
 * {@code opennms.minion.transport.sink.<type>}. Module ids not in the set
 * above throw {@link UnsupportedOperationException}; this prevents new
 * sinks from being silently misrouted.
 */
public class GrpcSinkDispatcherFactory implements MessageDispatcherFactory {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcSinkDispatcherFactory.class);

    private final ManagedChannel channel;
    private final MinionIdentity identity;
    private final SinkStreamRegistry streams = new SinkStreamRegistry();

    private final SyslogServiceGrpc.SyslogServiceStub syslogStub;
    private final TrapServiceGrpc.TrapServiceStub trapStub;
    private final TelemetryServiceGrpc.TelemetryServiceStub telemetryStub;

    public GrpcSinkDispatcherFactory(ManagedChannel channel, MinionIdentity identity) {
        this.channel = channel;
        this.identity = identity;
        MinionIdentityClientInterceptor interceptor =
            new MinionIdentityClientInterceptor(identity.getId(), identity.getLocation());
        this.syslogStub = SyslogServiceGrpc.newStub(channel).withInterceptors(interceptor);
        this.trapStub = TrapServiceGrpc.newStub(channel).withInterceptors(interceptor);
        this.telemetryStub = TelemetryServiceGrpc.newStub(channel).withInterceptors(interceptor);
    }

    @Override
    public <S extends Message, T extends Message> SyncDispatcher<S> createSyncDispatcher(SinkModule<S, T> module) {
        throw new UnsupportedOperationException(
            "GrpcSinkDispatcherFactory supports async dispatch only; got sync request for module '"
            + module.getId() + "'. Sync sinks (Heartbeat) use heartbeatDispatcherFactory.");
    }

    @Override
    public <S extends Message, T extends Message> AsyncDispatcher<S> createAsyncDispatcher(SinkModule<S, T> module) {
        String id = module.getId();
        return switch (id) {
            case "Syslog" -> syslogDispatcher(module);
            case "Trap" -> trapDispatcher(module);
            case "Telemetry-IPFIX" -> telemetryDispatcher(module, telemetryStub::publishIpfix);
            case "Telemetry-Netflow-5" -> telemetryDispatcher(module, telemetryStub::publishNetflow5);
            case "Telemetry-Netflow-9" -> telemetryDispatcher(module, telemetryStub::publishNetflow9);
            case "Telemetry-SFlow" -> telemetryDispatcher(module, telemetryStub::publishSflow);
            default -> throw new UnsupportedOperationException(
                "GrpcSinkDispatcherFactory has no route for module '" + id
                + "'. Supported: Syslog, Trap, Telemetry-IPFIX, Telemetry-Netflow-5, "
                + "Telemetry-Netflow-9, Telemetry-SFlow.");
        };
    }

    /**
     * Marshal a producer message via the module. Sinks routed through this factory
     * are non-aggregating so {@code S == T}; mirrors the unchecked cast precedent in
     * horizon's {@code AbstractMessageDispatcherFactory.DirectDispatcher.send}.
     */
    @SuppressWarnings("unchecked")
    private static <S extends Message, T extends Message> byte[] marshal(SinkModule<S, T> module, S message) {
        return module.marshal((T) message);
    }

    private <S extends Message, T extends Message> AsyncDispatcher<S> syslogDispatcher(SinkModule<S, T> module) {
        return new AsyncDispatcher<>() {
            @Override
            public CompletableFuture<DispatchStatus> send(S message) {
                StreamObserver<SyslogMessage> stream = streams.getOrOpen("Syslog", () ->
                    syslogStub.publish(noopAck("Syslog")));
                try {
                    stream.onNext(SyslogMessage.newBuilder()
                        .setPayload(ByteString.copyFrom(marshal(module, message)))
                        .setReceivedAt(now())
                        .build());
                    return CompletableFuture.completedFuture(DispatchStatus.QUEUED);
                } catch (RuntimeException e) {
                    LOG.warn("Syslog gRPC send failed; resetting stream", e);
                    streams.reset("Syslog");
                    return CompletableFuture.failedFuture(e);
                }
            }
            @Override public int getQueueSize() { return 0; }
            @Override public void close() {}
        };
    }

    private <S extends Message, T extends Message> AsyncDispatcher<S> trapDispatcher(SinkModule<S, T> module) {
        return new AsyncDispatcher<>() {
            @Override
            public CompletableFuture<DispatchStatus> send(S message) {
                StreamObserver<SnmpTrap> stream = streams.getOrOpen("Trap", () ->
                    trapStub.publish(noopAck("Trap")));
                try {
                    stream.onNext(SnmpTrap.newBuilder()
                        .setPayload(ByteString.copyFrom(marshal(module, message)))
                        .setReceivedAt(now())
                        .build());
                    return CompletableFuture.completedFuture(DispatchStatus.QUEUED);
                } catch (RuntimeException e) {
                    LOG.warn("Trap gRPC send failed; resetting stream", e);
                    streams.reset("Trap");
                    return CompletableFuture.failedFuture(e);
                }
            }
            @Override public int getQueueSize() { return 0; }
            @Override public void close() {}
        };
    }

    private <S extends Message, T extends Message> AsyncDispatcher<S> telemetryDispatcher(
            SinkModule<S, T> module,
            Function<StreamObserver<TelemetryAck>, StreamObserver<TelemetryDatagram>> opener) {
        String id = module.getId();
        return new AsyncDispatcher<>() {
            @Override
            public CompletableFuture<DispatchStatus> send(S message) {
                StreamObserver<TelemetryDatagram> stream = streams.getOrOpen(id, () ->
                    opener.apply(noopAck(id)));
                try {
                    stream.onNext(TelemetryDatagram.newBuilder()
                        .setPayload(ByteString.copyFrom(marshal(module, message)))
                        .setReceivedAt(now())
                        .build());
                    return CompletableFuture.completedFuture(DispatchStatus.QUEUED);
                } catch (RuntimeException e) {
                    LOG.warn("Telemetry gRPC send failed for {}; resetting stream", id, e);
                    streams.reset(id);
                    return CompletableFuture.failedFuture(e);
                }
            }
            @Override public int getQueueSize() { return 0; }
            @Override public void close() {}
        };
    }

    private <T> StreamObserver<T> noopAck(String moduleId) {
        return new StreamObserver<>() {
            @Override public void onNext(T value) { /* swallow ack */ }
            @Override public void onError(Throwable t) {
                LOG.warn("Sink stream {} error from gateway; resetting", moduleId, t);
                streams.reset(moduleId);
            }
            @Override public void onCompleted() {
                LOG.info("Sink stream {} closed by gateway", moduleId);
                streams.reset(moduleId);
            }
        };
    }

    private static Timestamp now() {
        Instant n = Instant.now();
        return Timestamp.newBuilder().setSeconds(n.getEpochSecond()).setNanos(n.getNano()).build();
    }
}
