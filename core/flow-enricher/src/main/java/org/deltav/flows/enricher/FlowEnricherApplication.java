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
package org.deltav.flows.enricher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Delta-V flow-enricher service.
 *
 * <p>Spring Boot 4.0 + Spring Cloud Stream (Kafka binder) consumer that reads
 * raw flow telemetry from Delta-V Sink topics, enriches each flow with node
 * lookup, locality, and SNMP-interface marking, and publishes enriched
 * FlowDocument protobuf messages to the {@code deltav-flows} topic — the
 * public contract topic for community downstream consumers.
 */
@SpringBootApplication
@EnableScheduling
public class FlowEnricherApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlowEnricherApplication.class, args);
    }
}
