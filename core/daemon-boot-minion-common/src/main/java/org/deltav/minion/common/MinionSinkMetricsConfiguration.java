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

import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;

/**
 * Transport-agnostic Sink metric wiring. Holds the Dropwizard
 * {@link MetricRegistry} used by horizon Sink module instrumentation
 * and the {@link HorizonMetricsBridge} that publishes those meters at
 * {@code /actuator/prometheus} under the {@code opennms_} prefix.
 *
 * <p>Extracted from the rc1 {@code KafkaSinkClientConfiguration} as
 * part of PR3's transport-coupled bean separation
 * (per {@code feedback_transport_coupled_subscriber_beans}). Both the
 * Kafka and gRPC sink dispatcher factories register against this
 * registry, so per-sink rollback flags don't disturb metric continuity.
 */
@Configuration
public class MinionSinkMetricsConfiguration {

    @Bean
    public MetricRegistry minionSinkMetricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge minionSinkMetricsBridge(MetricRegistry minionSinkMetricRegistry) {
        return new HorizonMetricsBridge(minionSinkMetricRegistry, "opennms");
    }
}
