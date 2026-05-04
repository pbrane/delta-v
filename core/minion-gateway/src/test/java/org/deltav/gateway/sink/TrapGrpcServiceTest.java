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
import org.deltav.minion.grpc.v1.SnmpTrap;
import org.deltav.minion.grpc.v1.TrapAck;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrapGrpcServiceTest {

    @Test
    void onNext_publishesToOpenNmsSinkTrapTopic() {
        SinkKafkaProducer producer = mock(SinkKafkaProducer.class);
        when(producer.send(eq("OpenNMS.Sink.Trap"), eq("Default@minion-A"),
                           argThat(b -> new String(b).equals("trap-bytes"))))
            .thenReturn(CompletableFuture.completedFuture(null));
        TrapGrpcService svc = new TrapGrpcService(producer);

        @SuppressWarnings("unchecked")
        StreamObserver<TrapAck> ack = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<SnmpTrap> in = svc.publish(ack);
            in.onNext(SnmpTrap.newBuilder()
                .setPayload(ByteString.copyFromUtf8("trap-bytes")).build());
        });

        verify(producer).send(eq("OpenNMS.Sink.Trap"), eq("Default@minion-A"),
            argThat(b -> new String(b).equals("trap-bytes")));
    }
}
