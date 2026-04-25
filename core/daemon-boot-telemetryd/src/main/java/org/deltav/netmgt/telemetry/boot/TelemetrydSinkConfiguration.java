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
package org.deltav.netmgt.telemetry.boot;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Sink configuration for Telemetryd multi-bridge pattern.
 *
 * <p>Provides a {@link TelemetryMessageConsumerManager} that spawns one
 * {@link KafkaSinkBridge} per telemetry protocol
 * (Netflow-5, IPFIX, sFlow, etc.). Each bridge consumes from its own Kafka
 * Sink topic (e.g., OpenNMS.Sink.Telemetry-Netflow-5).</p>
 *
 * <p>The {@link LocalMessageDispatcherFactory} routes dispatched messages
 * directly to the consumer manager in-process -- no remote transport.</p>
 */
@Configuration
public class TelemetrydSinkConfiguration {

    @Bean
    public TelemetryMessageConsumerManager messageConsumerManager(MeterRegistry meterRegistry) {
        return new TelemetryMessageConsumerManager(meterRegistry);
    }

    @Bean
    public MessageDispatcherFactory messageDispatcherFactory(
            TelemetryMessageConsumerManager messageConsumerManager) {
        return new LocalMessageDispatcherFactory(messageConsumerManager);
    }
}
