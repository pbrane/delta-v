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
package org.deltav.prometheus.writer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Delta-V prometheus-writer service.
 *
 * <p>Spring Boot 4.0 + Spring Cloud Stream consumer that reads
 * {@code TimeseriesBatch} protobuf records from the {@code deltav-timeseries}
 * Kafka topic, enriches each batch with node identity from a local
 * materialization of the compacted {@code deltav-node-context} topic,
 * translates to Prometheus Remote Write samples, and POSTs Snappy-compressed
 * protobuf batches to a configurable RW endpoint.
 *
 * <p>Phase 2 of the Kafka Time Series pipeline. See:
 * {@code docs/superpowers/specs/2026-04-17-kafka-ts-phase-2-prometheus-consumer-design.md}
 */
@SpringBootApplication(scanBasePackages = {"org.deltav.prometheus.writer"})
@EnableScheduling
public class PrometheusWriterApplication {

    public static void main(String[] args) {
        SpringApplication.run(PrometheusWriterApplication.class, args);
    }
}
