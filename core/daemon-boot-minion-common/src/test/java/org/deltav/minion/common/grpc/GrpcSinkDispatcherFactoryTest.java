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

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.SnmpTrap;
import org.deltav.minion.grpc.v1.SyslogAck;
import org.deltav.minion.grpc.v1.SyslogMessage;
import org.deltav.minion.grpc.v1.SyslogServiceGrpc;
import org.deltav.minion.grpc.v1.TelemetryAck;
import org.deltav.minion.grpc.v1.TelemetryDatagram;
import org.deltav.minion.grpc.v1.TelemetryServiceGrpc;
import org.deltav.minion.grpc.v1.TrapAck;
import org.deltav.minion.grpc.v1.TrapServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.ipc.sink.api.Message;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.distributed.core.api.MinionIdentity;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * In-process gRPC server round-trip test for {@link GrpcSinkDispatcherFactory}.
 *
 * <p>Uses programmatic lifecycle (BeforeEach/AfterEach) instead of JUnit 4
 * {@code GrpcCleanupRule} since this module is on JUnit 5 (Jupiter 6.x via
 * Spring Boot 4.0.3).
 */
class GrpcSinkDispatcherFactoryTest {

    private Server server;
    private ManagedChannel clientChannel;

    private CountDownLatch syslogLatch;
    private volatile SyslogMessage lastSyslog;

    private CountDownLatch trapLatch;
    private volatile SnmpTrap lastTrap;

    private CountDownLatch ipfixLatch;
    private volatile TelemetryDatagram lastIpfix;

    @BeforeEach
    void setUp() throws Exception {
        syslogLatch = new CountDownLatch(1);
        trapLatch = new CountDownLatch(1);
        ipfixLatch = new CountDownLatch(1);

        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor()
            .addService(new SyslogServiceGrpc.SyslogServiceImplBase() {
                @Override
                public StreamObserver<SyslogMessage> publish(StreamObserver<SyslogAck> ack) {
                    return new StreamObserver<>() {
                        @Override public void onNext(SyslogMessage value) {
                            lastSyslog = value;
                            syslogLatch.countDown();
                        }
                        @Override public void onError(Throwable t) {}
                        @Override public void onCompleted() { ack.onCompleted(); }
                    };
                }
            })
            .addService(new TrapServiceGrpc.TrapServiceImplBase() {
                @Override
                public StreamObserver<SnmpTrap> publish(StreamObserver<TrapAck> ack) {
                    return new StreamObserver<>() {
                        @Override public void onNext(SnmpTrap value) {
                            lastTrap = value;
                            trapLatch.countDown();
                        }
                        @Override public void onError(Throwable t) {}
                        @Override public void onCompleted() { ack.onCompleted(); }
                    };
                }
            })
            .addService(new TelemetryServiceGrpc.TelemetryServiceImplBase() {
                @Override
                public StreamObserver<TelemetryDatagram> publishIpfix(StreamObserver<TelemetryAck> ack) {
                    return new StreamObserver<>() {
                        @Override public void onNext(TelemetryDatagram value) {
                            lastIpfix = value;
                            ipfixLatch.countDown();
                        }
                        @Override public void onError(Throwable t) {}
                        @Override public void onCompleted() { ack.onCompleted(); }
                    };
                }
            })
            .build()
            .start();

        clientChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (clientChannel != null) {
            clientChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void syslogModule_dispatchesViaSyslogServiceStub() throws Exception {
        GrpcSinkDispatcherFactory factory = newFactory();
        @SuppressWarnings("unchecked")
        SinkModule<Message, Message> syslog = Mockito.mock(SinkModule.class);
        Mockito.when(syslog.getId()).thenReturn("Syslog");
        Mockito.when(syslog.marshal(Mockito.any())).thenReturn("hello-syslog".getBytes());

        AsyncDispatcher<Message> dispatcher = factory.createAsyncDispatcher(syslog);
        dispatcher.send(Mockito.mock(Message.class));

        assertThat(syslogLatch.await(5, TimeUnit.SECONDS))
            .as("syslog stream should have received one message")
            .isTrue();
        assertThat(lastSyslog.getPayload().toByteArray()).isEqualTo("hello-syslog".getBytes());
    }

    @Test
    void telemetryModuleIpfix_dispatchesViaPublishIpfixMethod() throws Exception {
        GrpcSinkDispatcherFactory factory = newFactory();
        @SuppressWarnings("unchecked")
        SinkModule<Message, Message> ipfix = Mockito.mock(SinkModule.class);
        Mockito.when(ipfix.getId()).thenReturn("Telemetry-IPFIX");
        Mockito.when(ipfix.marshal(Mockito.any())).thenReturn("ipfix-bytes".getBytes());

        AsyncDispatcher<Message> dispatcher = factory.createAsyncDispatcher(ipfix);
        dispatcher.send(Mockito.mock(Message.class));

        assertThat(ipfixLatch.await(5, TimeUnit.SECONDS))
            .as("IPFIX stream should have received one message")
            .isTrue();
        assertThat(lastIpfix.getPayload().toByteArray()).isEqualTo("ipfix-bytes".getBytes());
    }

    @Test
    void unknownModuleId_throwsUnsupportedOperationException() {
        GrpcSinkDispatcherFactory factory = newFactory();
        @SuppressWarnings("unchecked")
        SinkModule<Message, Message> unknown = Mockito.mock(SinkModule.class);
        Mockito.when(unknown.getId()).thenReturn("Unknown-Module");

        assertThatThrownBy(() -> factory.createAsyncDispatcher(unknown))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Unknown-Module");
    }

    private GrpcSinkDispatcherFactory newFactory() {
        MinionIdentity identity = Mockito.mock(MinionIdentity.class);
        Mockito.when(identity.getId()).thenReturn("minion-A");
        Mockito.when(identity.getLocation()).thenReturn("loc-DC1");
        return new GrpcSinkDispatcherFactory(clientChannel, identity);
    }
}
