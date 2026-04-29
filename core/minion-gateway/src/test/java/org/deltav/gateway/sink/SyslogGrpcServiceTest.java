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
import org.deltav.minion.grpc.v1.SyslogAck;
import org.deltav.minion.grpc.v1.SyslogMessage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SyslogGrpcServiceTest {

    @Test
    void onNext_publishesToOpenNmsSinkSyslogTopicWithIdentityKey() {
        SinkKafkaProducer producer = mock(SinkKafkaProducer.class);
        when(producer.send(eq("OpenNMS.Sink.Syslog"), eq("Default@minion-A"), argThat(b -> new String(b).equals("payload"))))
            .thenReturn(CompletableFuture.completedFuture(null));
        SyslogGrpcService svc = new SyslogGrpcService(producer);

        @SuppressWarnings("unchecked")
        StreamObserver<SyslogAck> ack = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<SyslogMessage> in = svc.publish(ack);
            in.onNext(SyslogMessage.newBuilder()
                .setPayload(ByteString.copyFromUtf8("payload"))
                .build());
        });

        verify(producer).send(eq("OpenNMS.Sink.Syslog"), eq("Default@minion-A"),
            argThat(b -> new String(b).equals("payload")));
    }

    @Test
    void streamCompleted_completesAckObserver() {
        SinkKafkaProducer producer = mock(SinkKafkaProducer.class);
        SyslogGrpcService svc = new SyslogGrpcService(producer);
        @SuppressWarnings("unchecked")
        StreamObserver<SyslogAck> ack = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<SyslogMessage> in = svc.publish(ack);
            in.onCompleted();
        });

        verify(ack).onCompleted();
    }
}
