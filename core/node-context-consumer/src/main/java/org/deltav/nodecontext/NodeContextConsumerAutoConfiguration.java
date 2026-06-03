/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.nodecontext;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

/**
 * Wires the shared NodeContext consumer for any service depending on this
 * module. Consumers need only the dependency plus spring.kafka.bootstrap-servers
 * (and optionally a topic override) — no component scan of this package.
 *
 * <p>The {@link NodeContextKafkaBootstrap} bean carries an
 * {@code @EventListener(ApplicationReadyEvent)} method, so Spring will
 * automatically start the consumer thread when the application is fully started.
 */
@AutoConfiguration
public class NodeContextConsumerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NodeContextCache nodeContextCache() {
        return new NodeContextCache();
    }

    @Bean
    @ConditionalOnMissingBean
    public NodeContextCacheHealthIndicator nodeContextCacheHealthIndicator(NodeContextCache cache) {
        return new NodeContextCacheHealthIndicator(cache);
    }

    @Bean
    @ConditionalOnMissingBean
    public NodeContextKafkaBootstrap nodeContextKafkaBootstrap(
            NodeContextCache cache,
            ApplicationEventPublisher eventPublisher,
            MeterRegistry meterRegistry,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new NodeContextKafkaBootstrap(cache, eventPublisher, meterRegistry, bootstrapServers);
    }
}
