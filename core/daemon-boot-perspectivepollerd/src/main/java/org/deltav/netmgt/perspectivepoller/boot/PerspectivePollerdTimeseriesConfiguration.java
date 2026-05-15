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
package org.deltav.netmgt.perspectivepoller.boot;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.kafka.clients.admin.NewTopic;
import org.deltav.poller.timeseries.ResponseTimePublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Phase 3: publishes per-poll response-time records for PerspectivePollerd to
 * the {@code deltav-timeseries} Kafka topic, mirroring the Pollerd publisher
 * shipped in PR #220 with {@code ProducerType.PRODUCER_PERSPECTIVE_POLLERD}
 * to disambiguate origin.
 *
 * <p>The enable flag is daemon-scoped — {@code deltav.perspective.timeseries.enabled}
 * — and intentionally distinct from Pollerd's {@code deltav.timeseries.enabled}.
 * The two daemons must remain independently togglable.
 *
 * <p>The topic itself is shared infrastructure (both daemons emit to the same
 * {@code deltav-timeseries} topic for prometheus-writer to consume). The
 * {@link NewTopic} declaration here is defensive: whichever daemon starts
 * first creates it, subsequent declarations are no-ops via
 * {@code TopicAlreadyExistsException}. Tuning knobs (partitions, retention)
 * follow the same first-declarer-wins semantics; operators changing them
 * should keep both daemons' values consistent.
 */
@Configuration
@ConditionalOnProperty(name = "deltav.perspective.timeseries.enabled", havingValue = "true")
public class PerspectivePollerdTimeseriesConfiguration {

    @Bean
    public NewTopic deltavTimeseriesTopic(
            @Value("${deltav.perspective.timeseries.partitions:16}") int partitions,
            @Value("${deltav.perspective.timeseries.replication-factor:1}") short replicationFactor,
            @Value("${deltav.perspective.timeseries.retention-days:1}") int retentionDays) {
        return TopicBuilder.name("deltav-timeseries")
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", String.valueOf(retentionDays * 24L * 3600_000L))
                .config("compression.type", "lz4")
                .build();
    }

    @Bean
    public ResponseTimePublisher responseTimePublisher(StreamBridge streamBridge,
                                                       MeterRegistry meterRegistry) {
        return new ResponseTimePublisher(streamBridge, meterRegistry);
    }
}
