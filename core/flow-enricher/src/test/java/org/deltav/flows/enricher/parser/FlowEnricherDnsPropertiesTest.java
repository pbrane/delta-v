/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class FlowEnricherDnsPropertiesTest {

    private static FlowEnricherDnsProperties bind(Map<String, Object> props) {
        return new Binder(new MapConfigurationPropertySource(props))
                .bind("deltav.flows.dns", Bindable.of(FlowEnricherDnsProperties.class))
                .get();
    }

    @Test
    void bindsHorizonDefaultsWhenNothingSet() {
        // Provide the bare minimum — an empty sub-tree forces Binder to construct
        // the record using all @DefaultValue annotations.
        FlowEnricherDnsProperties p = new Binder(new MapConfigurationPropertySource(Map.of()))
                .bindOrCreate("deltav.flows.dns", Bindable.of(FlowEnricherDnsProperties.class));
        assertThat(p.enabled()).isTrue();
        assertThat(p.scope()).isEqualTo(FlowEnricherDnsProperties.Scope.ALL);
        assertThat(p.nameservers()).isEmpty();
        assertThat(p.queryTimeoutMs()).isEqualTo(5000L);
        assertThat(p.maxConcurrent()).isEqualTo(1000);
        assertThat(p.cache().minTtlS()).isEqualTo(-1);
        assertThat(p.cache().maxTtlS()).isEqualTo(-1);
        assertThat(p.cache().negativeTtlS()).isEqualTo(-1);
        assertThat(p.circuitBreaker().enabled()).isTrue();
        assertThat(p.circuitBreaker().failureRateThreshold()).isEqualTo(80);
    }

    @Test
    void bindsOverridesIncludingRelaxedScopeEnum() {
        FlowEnricherDnsProperties p = bind(Map.of(
                "deltav.flows.dns.enabled", "false",
                "deltav.flows.dns.scope", "private",
                "deltav.flows.dns.nameservers", "8.8.8.8",
                "deltav.flows.dns.query-timeout-ms", "250",
                "deltav.flows.dns.max-concurrent", "500",
                "deltav.flows.dns.cache.negative-ttl-s", "120",
                "deltav.flows.dns.circuit-breaker.failure-rate-threshold", "50"
        ));
        assertThat(p.enabled()).isFalse();
        assertThat(p.scope()).isEqualTo(FlowEnricherDnsProperties.Scope.PRIVATE);
        assertThat(p.nameservers()).isEqualTo("8.8.8.8");
        assertThat(p.queryTimeoutMs()).isEqualTo(250L);
        assertThat(p.maxConcurrent()).isEqualTo(500);
        assertThat(p.cache().negativeTtlS()).isEqualTo(120);
        assertThat(p.circuitBreaker().failureRateThreshold()).isEqualTo(50);
    }
}
