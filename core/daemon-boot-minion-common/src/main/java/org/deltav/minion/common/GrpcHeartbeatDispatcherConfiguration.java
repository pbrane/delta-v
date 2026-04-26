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
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.deltav.minion.common.grpc.GrpcMessageDispatcherFactory;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.distributed.core.api.MinionIdentity;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Active when {@code opennms.minion.transport=grpc} (default). Builds a
 * Netty-based {@link ManagedChannel} pointed at the Envoy front-end of
 * minion-gateway and exposes a {@link MessageDispatcherFactory} qualified
 * as {@code heartbeatDispatcherFactory} - the bean name HeartbeatConfiguration
 * will inject in Task 7.
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.transport", havingValue = "grpc", matchIfMissing = true)
public class GrpcHeartbeatDispatcherConfiguration {

    private ManagedChannel channel;

    @Bean
    public ManagedChannel minionGatewayChannel(MinionProperties props) {
        this.channel = NettyChannelBuilder
            .forAddress(props.getGateway().getHost(), props.getGateway().getPort())
            .usePlaintext()                                    // h2c - Envoy fronts; rc1 has no TLS
            .keepAliveTime(30, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build();
        return channel;
    }

    @Bean(name = "heartbeatDispatcherFactory")
    public MessageDispatcherFactory heartbeatDispatcherFactory(
            ManagedChannel minionGatewayChannel, MinionIdentity identity) {
        return new GrpcMessageDispatcherFactory(minionGatewayChannel, identity);
    }

    @PreDestroy
    public void shutdown() {
        if (channel != null) {
            channel.shutdown();
        }
    }
}
