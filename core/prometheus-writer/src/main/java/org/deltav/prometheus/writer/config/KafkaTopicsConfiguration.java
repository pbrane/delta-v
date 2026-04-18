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
}
