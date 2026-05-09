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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deltav.gateway.kafka.HeartbeatKafkaProducer;
import org.deltav.minion.grpc.v1.Heartbeat;
import org.deltav.minion.grpc.v1.HeartbeatAck;
import org.deltav.minion.grpc.v1.HeartbeatServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class HeartbeatGrpcServiceTest {

    private HeartbeatKafkaProducer mockProducer;
    private ManagedChannel channel;
    private String serverName;

    @BeforeEach
    void setUp() throws Exception {
        mockProducer = mock(HeartbeatKafkaProducer.class);
        when(mockProducer.send(any())).thenReturn(CompletableFuture.completedFuture(null));

        serverName = InProcessServerBuilder.generateName();
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(ServerInterceptors.intercept(
                new HeartbeatGrpcService(mockProducer),
                new MinionIdentityServerInterceptor()))
            .build()
            .start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishStream_routesEachMessageToKafka_andAcksClient() throws Exception {
        Metadata headers = new Metadata();
        headers.put(MinionIdentityServerInterceptor.MINION_ID_HEADER, "minion-A");
        headers.put(MinionIdentityServerInterceptor.MINION_LOCATION_HEADER, "loc-DC1");

        var stub = HeartbeatServiceGrpc.newStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        CountDownLatch ackLatch = new CountDownLatch(1);
        StreamObserver<HeartbeatAck> ackObserver = new StreamObserver<>() {
            HeartbeatAck last;
            public void onNext(HeartbeatAck ack) { last = ack; ackLatch.countDown(); }
            public void onError(Throwable t) { throw new AssertionError(t); }
            public void onCompleted() {}
        };

        StreamObserver<Heartbeat> sender = stub.publish(ackObserver);
        sender.onNext(Heartbeat.newBuilder()
            .setMinionId("minion-A")
            .setLocation("loc-DC1")
            .setSentAt(Timestamp.newBuilder().setSeconds(1745625600L).build())
            .setVersion("1.2.0-rc1").build());

        assertThat(ackLatch.await(5, TimeUnit.SECONDS)).isTrue();

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
            ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(mockProducer).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("DeltaV.Sink.Heartbeat");
        assertThat(captor.getValue().key()).isEqualTo("loc-DC1@minion-A");

        sender.onCompleted();
    }

    @Test
    void publishStream_rejectsCallWithoutIdentityMetadata() {
        var stub = HeartbeatServiceGrpc.newStub(channel);

        CountDownLatch errorLatch = new CountDownLatch(1);
        StreamObserver<HeartbeatAck> observer = new StreamObserver<>() {
            public void onNext(HeartbeatAck ack) {}
            public void onError(Throwable t) {
                assertThat(t.getMessage()).contains("UNAUTHENTICATED");
                errorLatch.countDown();
            }
            public void onCompleted() {}
        };

        var sender = stub.publish(observer);
        sender.onNext(Heartbeat.newBuilder().setMinionId("x").build());

        try { assertThat(errorLatch.await(5, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
