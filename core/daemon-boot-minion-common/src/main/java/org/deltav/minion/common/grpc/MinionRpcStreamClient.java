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
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.RpcRequest;
import org.deltav.minion.grpc.v1.RpcResponse;
import org.opennms.core.rpc.api.RpcModule;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Minion-side gRPC RPC handler. Receives {@link RpcRequest} messages on a
 * bidi stream from minion-gateway, dispatches them via the local
 * {@link RpcModule} registry, and sends marshaled {@link RpcResponse}
 * messages back on the same stream.
 *
 * <p>Idempotency is a contract on the modules registered here, not a
 * property enforced by this client. Per Decision 1 sub-decision 1-ii of
 * the v1.2.0-rc2 decisions doc: all RpcModules must be idempotent
 * because gateway-side redispatch on stream close can cause at-least-once
 * execution.
 */
public class MinionRpcStreamClient {

    private static final Logger LOG = LoggerFactory.getLogger(MinionRpcStreamClient.class);

    private final Map<String, RpcModule> registry;
    private final MinionIdentity identity;
    private final Supplier<StreamObserver<RpcResponse>> outboundSupplier;

    public MinionRpcStreamClient(Map<String, RpcModule> registry,
                                 MinionIdentity identity,
                                 Supplier<StreamObserver<RpcResponse>> outboundSupplier) {
        this.registry = registry;
        this.identity = identity;
        this.outboundSupplier = outboundSupplier;
    }

    /**
     * Returns a {@link StreamObserver} that receives {@link RpcRequest} messages from
     * minion-gateway and dispatches them to the appropriate {@link RpcModule}.
     *
     * <p>The outbound supplier is evaluated lazily — once per {@code onNext} call, not at
     * construction time. This is intentional: {@link org.deltav.minion.common.GrpcRpcStreamConfiguration}
     * constructs this client before the bidi stream is opened, then opens the stream in
     * {@code @PostConstruct}. The supplier captures a field reference ({@code () -> outboundStream})
     * that resolves to {@code null} at construction time but to the real stream observer
     * by the time the first {@link RpcRequest} arrives from the gateway.</p>
     */
    public StreamObserver<RpcRequest> streamObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(RpcRequest req) {
                StreamObserver<RpcResponse> outbound = outboundSupplier.get();
                if (outbound == null) {
                    LOG.warn("Outbound stream not yet ready; dropping rpcId={}", req.getRpcId());
                    return;
                }
                handle(req, outbound);
            }
            @Override
            public void onError(Throwable t) {
                LOG.info("RPC stream error on minion={}: {}", identity.getId(), t.toString());
            }
            @Override
            public void onCompleted() {
                LOG.info("RPC stream closed on minion={}", identity.getId());
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void handle(RpcRequest req, StreamObserver<RpcResponse> outbound) {
        String moduleId = req.getModuleId();
        RpcModule module = registry.get(moduleId);
        if (module == null) {
            synchronized (outbound) {
                outbound.onNext(errorResponse(req.getRpcId(), "Unknown module: " + moduleId));
            }
            return;
        }
        try {
            String payloadStr = req.getPayload().toStringUtf8();
            org.opennms.core.rpc.api.RpcRequest internal = (org.opennms.core.rpc.api.RpcRequest)
                module.unmarshalRequest(payloadStr);
            module.execute(internal).whenComplete((rsp, t) -> {
                if (t != null) {
                    synchronized (outbound) {
                        outbound.onNext(errorResponse(req.getRpcId(), t.toString()));
                    }
                } else {
                    try {
                        @SuppressWarnings("unchecked")
                        String marshaled = module.marshalResponse(
                            (org.opennms.core.rpc.api.RpcResponse) rsp);
                        RpcResponse response = RpcResponse.newBuilder()
                            .setRpcId(req.getRpcId())
                            .setPayload(ByteString.copyFromUtf8(marshaled))
                            .setCompletedAt(now())
                            .build();
                        synchronized (outbound) {
                            outbound.onNext(response);
                        }
                    } catch (Throwable marshalError) {
                        LOG.warn("marshalResponse failed for rpcId={}: {}", req.getRpcId(), marshalError.toString());
                        synchronized (outbound) {
                            outbound.onNext(errorResponse(req.getRpcId(), marshalError.toString()));
                        }
                    }
                }
            });
        } catch (Throwable t) {
            synchronized (outbound) {
                outbound.onNext(errorResponse(req.getRpcId(), t.toString()));
            }
        }
    }

    private RpcResponse errorResponse(String rpcId, String error) {
        return RpcResponse.newBuilder()
            .setRpcId(rpcId)
            .setErrorMessage(error)
            .setCompletedAt(now())
            .build();
    }

    private Timestamp now() {
        Instant n = Instant.now();
        return Timestamp.newBuilder().setSeconds(n.getEpochSecond()).setNanos(n.getNano()).build();
    }
}
