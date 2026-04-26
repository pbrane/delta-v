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
package org.deltav.gateway;

import com.codahale.metrics.MetricRegistry;
import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class MinionGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(MinionGatewayApplication.class, args);
    }

    @Bean
    public MetricRegistry horizonMetricRegistry() {
        return new MetricRegistry();
    }

    /**
     * Bridges horizon's Dropwizard MetricRegistry (used by the Kafka producer
     * republishing to OpenNMS.Sink.Heartbeat) into Micrometer so meters appear
     * at /actuator/prometheus under the "opennms_" prefix per
     * feedback_meter_naming_horizon_vs_deltav. Spring gRPC's native
     * Micrometer meters (grpc.server.*) are emitted directly without
     * needing this bridge.
     */
    @Bean
    public HorizonMetricsBridge horizonMetricsBridge(MetricRegistry horizonMetricRegistry) {
        return new HorizonMetricsBridge(horizonMetricRegistry, "opennms");
    }
}
