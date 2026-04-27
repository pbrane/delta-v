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

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.deltav.gateway.rpc.MinionStreamPool;
import org.deltav.gateway.rpc.RpcResponseHandler;
import org.deltav.gateway.rpc.RpcStreamCloseHandler;
import org.deltav.minion.grpc.v1.RpcRequest;
import org.deltav.minion.grpc.v1.RpcResponse;
import org.junit.jupiter.api.Test;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RpcChannelGrpcServiceTest {

    @Test
    void streamOpen_registersInPool() {
        MinionStreamPool pool = mock(MinionStreamPool.class);
        RpcStreamCloseHandler closeHandler = mock(RpcStreamCloseHandler.class);
        RpcResponseHandler responseHandler = mock(RpcResponseHandler.class);
        RpcChannelGrpcService svc = new RpcChannelGrpcService(pool, closeHandler, responseHandler);

        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> requestObserver = mock(StreamObserver.class);

        Context ctx = Context.current()
                .withValue(MINION_ID_CTX, "minion-A")
                .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> svc.channel(requestObserver));

        verify(pool).register("Default", "minion-A", requestObserver);
    }

    @Test
    void streamClose_unregistersAndTriggersRedispatch() {
        MinionStreamPool pool = mock(MinionStreamPool.class);
        RpcStreamCloseHandler closeHandler = mock(RpcStreamCloseHandler.class);
        RpcResponseHandler responseHandler = mock(RpcResponseHandler.class);
        RpcChannelGrpcService svc = new RpcChannelGrpcService(pool, closeHandler, responseHandler);

        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> requestObserver = mock(StreamObserver.class);

        Context ctx = Context.current()
                .withValue(MINION_ID_CTX, "minion-A")
                .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<RpcResponse> responseObserver = svc.channel(requestObserver);
            responseObserver.onCompleted();
        });

        verify(pool).unregister("Default", "minion-A");
        verify(closeHandler).onStreamClosed("Default", requestObserver);
    }

    @Test
    void incomingResponse_forwardedToResponseHandler() {
        MinionStreamPool pool = mock(MinionStreamPool.class);
        RpcStreamCloseHandler closeHandler = mock(RpcStreamCloseHandler.class);
        RpcResponseHandler responseHandler = mock(RpcResponseHandler.class);
        RpcChannelGrpcService svc = new RpcChannelGrpcService(pool, closeHandler, responseHandler);

        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> requestObserver = mock(StreamObserver.class);

        Context ctx = Context.current()
                .withValue(MINION_ID_CTX, "minion-A")
                .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<RpcResponse> responseObserver = svc.channel(requestObserver);
            RpcResponse rsp = RpcResponse.newBuilder().setRpcId("xyz").build();
            responseObserver.onNext(rsp);
        });

        verify(responseHandler).handle(RpcResponse.newBuilder().setRpcId("xyz").build());
    }
}
