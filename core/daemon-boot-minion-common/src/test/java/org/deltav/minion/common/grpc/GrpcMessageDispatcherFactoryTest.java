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
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.Heartbeat;
import org.deltav.minion.grpc.v1.HeartbeatAck;
import org.deltav.minion.grpc.v1.HeartbeatServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opennms.core.ipc.sink.api.Message;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.ipc.sink.api.SyncDispatcher;
import org.opennms.distributed.core.api.MinionIdentity;
import org.opennms.minion.heartbeat.common.HeartbeatModule;
import org.opennms.minion.heartbeat.common.MinionIdentityDTO;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcMessageDispatcherFactoryTest {

    private ManagedChannel clientChannel;
    private CountDownLatch heartbeatReceived;
    private volatile Heartbeat lastReceived;

    @BeforeEach
    void setUp() throws Exception {
        heartbeatReceived = new CountDownLatch(1);
        String name = InProcessServerBuilder.generateName();

        InProcessServerBuilder.forName(name).directExecutor()
            .addService(new HeartbeatServiceGrpc.HeartbeatServiceImplBase() {
                @Override
                public StreamObserver<Heartbeat> publish(StreamObserver<HeartbeatAck> ack) {
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(Heartbeat hb) {
                            lastReceived = hb;
                            heartbeatReceived.countDown();
                            ack.onNext(HeartbeatAck.newBuilder().setMinionId(hb.getMinionId()).build());
                        }
                        @Override
                        public void onError(Throwable t) {
                        }
                        @Override
                        public void onCompleted() {
                            ack.onCompleted();
                        }
                    };
                }
            })
            .build().start();

        clientChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @Test
    void send_translatesAndStreamsHeartbeatPayload() throws Exception {
        MinionIdentity id = Mockito.mock(MinionIdentity.class);
        Mockito.when(id.getId()).thenReturn("minion-A");
        Mockito.when(id.getLocation()).thenReturn("loc-DC1");

        var factory = new GrpcMessageDispatcherFactory(clientChannel, id);
        SyncDispatcher<MinionIdentityDTO> dispatcher = factory.createSyncDispatcher(new HeartbeatModule());

        MinionIdentityDTO dto = new MinionIdentityDTO();
        dto.setId("minion-A");
        dto.setLocation("loc-DC1");
        dto.setVersion("1.2.0-rc1");
        dto.setTimestamp(new Date(1745625600000L));

        dispatcher.send(dto);

        assertThat(heartbeatReceived.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(lastReceived.getMinionId()).isEqualTo("minion-A");
        assertThat(lastReceived.getLocation()).isEqualTo("loc-DC1");
        assertThat(lastReceived.getVersion()).isEqualTo("1.2.0-rc1");

        dispatcher.close();
    }

    @Test
    void createSyncDispatcher_rejectsNonHeartbeatModules() {
        MinionIdentity id = Mockito.mock(MinionIdentity.class);
        Mockito.when(id.getId()).thenReturn("m");
        Mockito.when(id.getLocation()).thenReturn("l");

        var factory = new GrpcMessageDispatcherFactory(clientChannel, id);
        @SuppressWarnings("unchecked")
        SinkModule<Message, Message> fakeSyslog = Mockito.mock(SinkModule.class);
        Mockito.when(fakeSyslog.getId()).thenReturn("Syslog");

        assertThatThrownBy(() -> factory.createSyncDispatcher(fakeSyslog))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Heartbeat")
            .hasMessageContaining("Syslog");
    }
}
