/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.flows.enricher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.flows.enricher.parser.DeltavNettyDnsResolver;
import org.deltav.flows.enricher.parser.FlowEnricherDnsProperties;
import org.deltav.flows.enricher.parser.LocalityFilteringDnsResolver;
import org.deltav.flows.enricher.parser.NoOpDnsResolver;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;

class FlowEnricherDnsWiringTest {

    private final FlowEnricherConfiguration cfg = new FlowEnricherConfiguration();

    private static FlowEnricherDnsProperties props(boolean enabled, FlowEnricherDnsProperties.Scope scope) {
        return new FlowEnricherDnsProperties(enabled, scope, "", 5000L, 1000,
                new FlowEnricherDnsProperties.Cache(-1, -1, -1),
                new FlowEnricherDnsProperties.CircuitBreaker(true, 80));
    }

    @Test
    void enabledYieldsLocalityFilteringDecorator() {
        DnsResolver r = cfg.flowParserDnsResolver(
                mock(DeltavNettyDnsResolver.class),
                props(true, FlowEnricherDnsProperties.Scope.ALL),
                new SimpleMeterRegistry());
        assertThat(r).isInstanceOf(LocalityFilteringDnsResolver.class);
    }

    @Test
    void disabledYieldsNoOpResolver() {
        DnsResolver r = cfg.noOpFlowParserDnsResolver();
        assertThat(r).isInstanceOf(NoOpDnsResolver.class);
    }
}
