/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Curated, daemon-scoped tuning for the flow-enricher's reverse-DNS enrichment.
 * Defaults mirror horizon's {@code NettyDnsResolver} field defaults. The obscure
 * NettyDnsResolver knobs (numContexts, ring buffers, breaker wait, bulkhead
 * max-wait) are intentionally NOT exposed and stay at their horizon defaults;
 * {@code bulkheadMaxWaitDurationMillis} is derived as {@code queryTimeoutMs + 100}
 * at wiring time to preserve horizon's relationship.
 */
@ConfigurationProperties("deltav.flows.dns")
public record FlowEnricherDnsProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("all") Scope scope,
        @DefaultValue("") String nameservers,
        @DefaultValue("5000") long queryTimeoutMs,
        @DefaultValue("1000") int maxConcurrent,
        @DefaultValue Cache cache,
        @DefaultValue CircuitBreaker circuitBreaker) {

    /** Which addresses the locality gate allows through to the resolver. */
    public enum Scope { ALL, PRIVATE }

    public record Cache(
            @DefaultValue("-1") int minTtlS,
            @DefaultValue("-1") int maxTtlS,
            @DefaultValue("-1") int negativeTtlS) {}

    public record CircuitBreaker(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("80") int failureRateThreshold) {}
}
