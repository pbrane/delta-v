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
package org.deltav.netmgt.poller.boot;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.kafka.clients.admin.NewTopic;
import org.deltav.timeseries.proto.ProducerType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Phase 3: publishes per-poll response-time records to the
 * {@code deltav-timeseries} Kafka topic, mirroring the collectd publisher
 * shipped in PR #170.
 *
 * <p>Activated only when {@code deltav.timeseries.enabled=true}; otherwise
 * the publisher bean is absent and {@link InstrumentedPollContext} sees a
 * null publisher and skips Phase 3 work. Phase 2 Micrometer counters fire
 * unconditionally regardless of this flag.
 */
@Configuration
@ConditionalOnProperty(name = "deltav.timeseries.enabled", havingValue = "true")
public class PollerdTimeseriesConfiguration {

    /**
     * Provisions the {@code deltav-timeseries} topic. The collectd publisher
     * declares the same topic; whichever daemon starts first creates it,
     * subsequent daemons see {@code TopicAlreadyExistsException} which Kafka
     * Admin treats as a no-op.
     */
    @Bean
    public NewTopic deltavTimeseriesTopic(
            @Value("${deltav.timeseries.partitions:16}") int partitions,
            @Value("${deltav.timeseries.replication-factor:1}") short replicationFactor,
            @Value("${deltav.timeseries.retention-days:1}") int retentionDays) {
        return TopicBuilder.name("deltav-timeseries")
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", String.valueOf(retentionDays * 24L * 3600_000L))
                .config("compression.type", "lz4")
                .build();
    }

    @Bean
    public PollResultPublisher pollResultPublisher(StreamBridge streamBridge,
                                                   MeterRegistry meterRegistry) {
        return new PollResultPublisher(streamBridge, meterRegistry,
                ProducerType.PRODUCER_POLLERD, "pollerd");
    }
}
