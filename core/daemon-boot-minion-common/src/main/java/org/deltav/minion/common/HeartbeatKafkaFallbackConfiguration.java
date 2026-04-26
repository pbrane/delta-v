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

import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@code MINION_TRANSPORT=kafka} rollback path: aliases the primary Kafka
 * {@link MessageDispatcherFactory} under the {@code heartbeatDispatcherFactory}
 * bean name so HeartbeatConfiguration's {@code @Qualifier} injection still
 * resolves regardless of selected transport.
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.transport", havingValue = "kafka")
public class HeartbeatKafkaFallbackConfiguration {

    @Bean(name = "heartbeatDispatcherFactory")
    public MessageDispatcherFactory heartbeatDispatcherFactory(
            MessageDispatcherFactory kafkaPrimary) {
        return kafkaPrimary;
    }
}
