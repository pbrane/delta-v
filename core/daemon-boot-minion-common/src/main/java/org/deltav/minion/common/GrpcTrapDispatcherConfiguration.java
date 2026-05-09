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
import org.deltav.minion.common.grpc.GrpcSinkDispatcherFactory;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.distributed.core.api.MinionIdentity;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * gRPC-backed SNMP Trap sink dispatcher factory. Reuses {@code minionGatewayChannel}
 * provided by {@link GrpcHeartbeatDispatcherConfiguration}.
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.transport.sink.trap",
                       havingValue = "grpc", matchIfMissing = true)
public class GrpcTrapDispatcherConfiguration {

    @Bean(name = "trapDispatcherFactory")
    public MessageDispatcherFactory trapDispatcherFactory(
            @Qualifier("minionGatewayChannel") ManagedChannel minionGatewayChannel,
            MinionIdentity identity) {
        return new GrpcSinkDispatcherFactory(minionGatewayChannel, identity);
    }
}
