/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

@Configuration
public class KafkaTopicsConfiguration {

    @Bean
    public NewTopic deltavPrometheusWriterDlqTopic() {
        return TopicBuilder.name("deltav-prometheus-writer-dlq")
                .partitions(16)
                .replicas(1)
                .configs(Map.of(
                        "cleanup.policy", "delete",
                        "retention.ms", "604800000",          // 7 days
                        "compression.type", "lz4"))
                .build();
    }

    /**
     * Mirrors the Collectd-side {@code deltavTimeseriesTopic} declaration. Without
     * this, the SCS Kafka binder's consumer auto-creates {@code deltav-timeseries}
     * with the broker default (4 partitions) before Collectd's {@code KafkaAdmin}
     * can run its 16-partition declaration. Kafka's partition expansion does not
     * automatically rebalance the existing consumer group: the consumer stays
     * assigned to partitions 0-3 forever and silently misses records hashed to
     * partitions 4-15. Pre-declaring the topic here lets prometheus-writer's
     * {@code KafkaAdmin} converge on the correct partition count BEFORE the
     * binder's consumer subscribes, eliminating the race.
     *
     * <p>Partition / replication / retention / compression settings must match
     * the Collectd-side declaration in {@code TimeseriesKafkaPublisherConfiguration}.
     */
    @Bean
    public NewTopic deltavTimeseriesTopic() {
        return TopicBuilder.name("deltav-timeseries")
                .partitions(16)
                .replicas(1)
                .configs(Map.of(
                        "cleanup.policy", "delete",
                        "retention.ms", "86400000",           // 1 day (matches Collectd default)
                        "compression.type", "lz4"))
                .build();
    }

    /**
     * Mirrors the Provisiond-side {@code deltavNodeContextTopic} declaration so
     * the same auto-create-vs-declare race that bites {@code deltav-timeseries}
     * cannot bite {@code deltav-node-context}. Defensive declaration here keeps
     * both source-topic schemas consistent regardless of startup ordering.
     *
     * <p>Settings match Provisiond's declaration in NodeContextProducerConfiguration:
     * 8 partitions, compacted, retain forever (-1), min compaction lag 1m,
     * delete retention 1d.
     */
    @Bean
    public NewTopic deltavNodeContextTopic() {
        return TopicBuilder.name("deltav-node-context")
                .partitions(8)
                .replicas(1)
                .configs(Map.of(
                        "cleanup.policy", "compact",
                        "retention.ms", "-1",
                        "min.compaction.lag.ms", "60000",     // 1 min
                        "delete.retention.ms", "86400000"))   // 1 day
                .build();
    }
}
