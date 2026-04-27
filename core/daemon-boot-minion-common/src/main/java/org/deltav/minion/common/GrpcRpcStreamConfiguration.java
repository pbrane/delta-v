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
package org.deltav.minion.common;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.common.grpc.MinionIdentityClientInterceptor;
import org.deltav.minion.common.grpc.MinionRpcStreamClient;
import org.deltav.minion.grpc.v1.RpcChannelServiceGrpc;
import org.deltav.minion.grpc.v1.RpcRequest;
import org.deltav.minion.grpc.v1.RpcResponse;
import org.opennms.core.rpc.api.RpcModule;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Active when {@code opennms.minion.transport.rpc=grpc} (default). Wires a
 * {@link MinionRpcStreamClient} that opens a bidi gRPC stream to minion-gateway
 * and dispatches inbound {@link RpcRequest} messages to the registered
 * {@link RpcModule} beans.
 *
 * <p>This configuration reuses the {@code minionGatewayChannel} bean provided by
 * {@link GrpcHeartbeatDispatcherConfiguration} rather than creating a second channel.
 * A {@link MinionIdentityClientInterceptor} is constructed from the injected
 * {@link MinionIdentity} so that outbound RPC calls carry the required
 * {@code x-minion-id} / {@code x-minion-location} metadata.</p>
 *
 * <p>The outbound {@link StreamObserver} is assigned in the {@link SmartLifecycle#start()}
 * callback (phase 400, after Kafka RPC phase 300) via a field reference captured by a
 * lambda supplier ({@code () -> outboundStream}). Java evaluates the field value on each
 * lambda invocation, not at lambda creation time, so the supplier correctly resolves
 * {@code null} before stream open and the live observer after. This lazy evaluation is
 * intentional — see {@link MinionRpcStreamClient#streamObserver()} Javadoc for details.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.transport.rpc", havingValue = "grpc", matchIfMissing = true)
public class GrpcRpcStreamConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcRpcStreamConfiguration.class);

    private final ManagedChannel channel;
    private final MinionIdentity identity;
    @SuppressWarnings("rawtypes")
    private final List<RpcModule> modules;

    private volatile StreamObserver<RpcResponse> outboundStream;

    @SuppressWarnings("rawtypes")
    public GrpcRpcStreamConfiguration(
            @Qualifier("minionGatewayChannel") ManagedChannel channel,
            MinionIdentity identity,
            List<RpcModule> modules) {
        this.channel = channel;
        this.identity = identity;
        this.modules = modules;
    }

    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public MinionRpcStreamClient minionRpcStreamClient() {
        Map<String, RpcModule> registry = modules.stream()
            .collect(Collectors.toMap(
                RpcModule::getId,
                m -> (RpcModule) m,
                (existing, duplicate) -> existing));
        return new MinionRpcStreamClient(registry, identity, () -> outboundStream);
    }

    /**
     * Lifecycle bean that opens the bidi gRPC stream to minion-gateway at phase 400
     * (after the Kafka RPC server at phase 300). The call sequence in {@code start()} is:
     * <ol>
     *   <li>Obtain the inbound {@link StreamObserver} from {@link MinionRpcStreamClient#streamObserver()}.
     *       The client does not evaluate the outbound supplier at this point.</li>
     *   <li>Open the bidi stream via {@code stub.channel(inbound)}, which returns the
     *       outbound observer the Minion uses to send {@link RpcResponse} messages back.</li>
     *   <li>Assign the outbound observer to {@link #outboundStream} so the lazy supplier
     *       resolves it on the first inbound {@link RpcRequest}.</li>
     * </ol>
     */
    @Bean
    public SmartLifecycle rpcStreamLifecycle(MinionRpcStreamClient client) {
        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                MinionIdentityClientInterceptor identityInterceptor =
                    new MinionIdentityClientInterceptor(identity.getId(), identity.getLocation());
                RpcChannelServiceGrpc.RpcChannelServiceStub stub =
                    RpcChannelServiceGrpc.newStub(channel).withInterceptors(identityInterceptor);
                StreamObserver<RpcRequest> inbound = client.streamObserver();
                outboundStream = stub.channel(inbound);
                running = true;
                LOG.info("RPC stream opened to minion-gateway for minion={} location={}",
                    identity.getId(), identity.getLocation());
            }

            @Override
            public void stop() {
                if (outboundStream != null) {
                    outboundStream.onCompleted();
                    outboundStream = null;
                }
                running = false;
                LOG.info("RPC stream closed for minion={}", identity.getId());
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 400;
            }
        };
    }
}
