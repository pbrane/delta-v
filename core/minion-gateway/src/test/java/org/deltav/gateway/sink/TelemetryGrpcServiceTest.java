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

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.TelemetryAck;
import org.deltav.minion.grpc.v1.TelemetryDatagram;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TelemetryGrpcServiceTest {

    @Test
    void publishIpfix_routesToIpfixTopicWithIdentityKey() {
        SinkKafkaProducer producer = mock(SinkKafkaProducer.class);
        when(producer.send(eq("DeltaV.Sink.Telemetry-IPFIX"), eq("Default@minion-A"),
                           argThat(b -> new String(b).equals("ipfix-bytes"))))
            .thenReturn(CompletableFuture.completedFuture(null));
        TelemetryGrpcService svc = new TelemetryGrpcService(producer);

        @SuppressWarnings("unchecked")
        StreamObserver<TelemetryAck> ack = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<TelemetryDatagram> in = svc.publishIpfix(ack);
            in.onNext(TelemetryDatagram.newBuilder()
                .setPayload(ByteString.copyFromUtf8("ipfix-bytes")).build());
        });

        verify(producer).send(eq("DeltaV.Sink.Telemetry-IPFIX"), eq("Default@minion-A"),
            argThat(b -> new String(b).equals("ipfix-bytes")));
    }

    @Test
    void publishNetflow5_routesToNetflow5TopicWithIdentityKey() {
        SinkKafkaProducer producer = mock(SinkKafkaProducer.class);
        when(producer.send(eq("DeltaV.Sink.Telemetry-Netflow-5"), eq("Default@minion-A"),
                           argThat(b -> new String(b).equals("nf5-bytes"))))
            .thenReturn(CompletableFuture.completedFuture(null));
        TelemetryGrpcService svc = new TelemetryGrpcService(producer);

        @SuppressWarnings("unchecked")
        StreamObserver<TelemetryAck> ack = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<TelemetryDatagram> in = svc.publishNetflow5(ack);
            in.onNext(TelemetryDatagram.newBuilder()
                .setPayload(ByteString.copyFromUtf8("nf5-bytes")).build());
        });

        verify(producer).send(eq("DeltaV.Sink.Telemetry-Netflow-5"), eq("Default@minion-A"),
            argThat(b -> new String(b).equals("nf5-bytes")));
    }

    @Test
    void publishNetflow9_routesToNetflow9TopicWithIdentityKey() {
        SinkKafkaProducer producer = mock(SinkKafkaProducer.class);
        when(producer.send(eq("DeltaV.Sink.Telemetry-Netflow-9"), eq("Default@minion-A"),
                           argThat(b -> new String(b).equals("nf9-bytes"))))
            .thenReturn(CompletableFuture.completedFuture(null));
        TelemetryGrpcService svc = new TelemetryGrpcService(producer);

        @SuppressWarnings("unchecked")
        StreamObserver<TelemetryAck> ack = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<TelemetryDatagram> in = svc.publishNetflow9(ack);
            in.onNext(TelemetryDatagram.newBuilder()
                .setPayload(ByteString.copyFromUtf8("nf9-bytes")).build());
        });

        verify(producer).send(eq("DeltaV.Sink.Telemetry-Netflow-9"), eq("Default@minion-A"),
            argThat(b -> new String(b).equals("nf9-bytes")));
    }

    @Test
    void publishSflow_routesToSflowTopicWithIdentityKey() {
        SinkKafkaProducer producer = mock(SinkKafkaProducer.class);
        when(producer.send(eq("DeltaV.Sink.Telemetry-SFlow"), eq("Default@minion-A"),
                           argThat(b -> new String(b).equals("sflow-bytes"))))
            .thenReturn(CompletableFuture.completedFuture(null));
        TelemetryGrpcService svc = new TelemetryGrpcService(producer);

        @SuppressWarnings("unchecked")
        StreamObserver<TelemetryAck> ack = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<TelemetryDatagram> in = svc.publishSflow(ack);
            in.onNext(TelemetryDatagram.newBuilder()
                .setPayload(ByteString.copyFromUtf8("sflow-bytes")).build());
        });

        verify(producer).send(eq("DeltaV.Sink.Telemetry-SFlow"), eq("Default@minion-A"),
            argThat(b -> new String(b).equals("sflow-bytes")));
    }
}
