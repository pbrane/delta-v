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

import io.grpc.stub.StreamObserver;
import org.deltav.gateway.rpc.MinionStreamPool;
import org.deltav.gateway.rpc.RpcResponseHandler;
import org.deltav.gateway.rpc.RpcStreamCloseHandler;
import org.deltav.minion.grpc.v1.RpcChannelServiceGrpc;
import org.deltav.minion.grpc.v1.RpcRequest;
import org.deltav.minion.grpc.v1.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;

@GrpcService
public class RpcChannelGrpcService extends RpcChannelServiceGrpc.RpcChannelServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(RpcChannelGrpcService.class);

    private final MinionStreamPool pool;
    private final RpcStreamCloseHandler closeHandler;
    private final RpcResponseHandler responseHandler;

    public RpcChannelGrpcService(MinionStreamPool pool,
                                 RpcStreamCloseHandler closeHandler,
                                 RpcResponseHandler responseHandler) {
        this.pool = pool;
        this.closeHandler = closeHandler;
        this.responseHandler = responseHandler;
    }

    /**
     * Registers a Minion's bidi stream in the pool and returns the inbound observer
     * for {@link RpcResponse} messages. Per Decision 1 of v1.2.0-rc2: there is an
     * unavoidable race where a dispatcher thread may call {@link MinionStreamPool#pickStream}
     * shortly after register() but before the Minion is fully ready. The redispatch
     * handler (triggered on stream close mid-flight) absorbs this by re-queuing the
     * RPC to a sibling stream — no correctness fix needed at this layer.
     */
    @Override
    public StreamObserver<RpcResponse> channel(StreamObserver<RpcRequest> requestObserver) {
        String minionId = MINION_ID_CTX.get();
        String location = MINION_LOCATION_CTX.get();
        LOG.info("RPC stream opened for minion={} location={}", minionId, location);

        pool.register(location, minionId, requestObserver);

        return new StreamObserver<>() {
            @Override
            public void onNext(RpcResponse response) {
                responseHandler.handle(response);
            }

            @Override
            public void onError(Throwable t) {
                LOG.info("RPC stream error for minion={}: {}", minionId, t.toString());
                pool.unregister(location, minionId);
                closeHandler.onStreamClosed(location, requestObserver);
                // Intentionally NOT relaying error back via requestObserver.onError():
                // the gRPC runtime already surfaces the error to the client via the
                // stream context, and relaying would duplicate the signal. Matches
                // HeartbeatGrpcService convention.
            }

            @Override
            public void onCompleted() {
                LOG.info("RPC stream closed for minion={}", minionId);
                pool.unregister(location, minionId);
                closeHandler.onStreamClosed(location, requestObserver);
                requestObserver.onCompleted();
            }
        };
    }
}
