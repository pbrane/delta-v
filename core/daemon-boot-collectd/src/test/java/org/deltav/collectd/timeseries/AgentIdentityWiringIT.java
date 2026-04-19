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
package org.deltav.collectd.timeseries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.deltav.collectd.identity.AgentIdentity;
import org.deltav.collectd.identity.AgentIdentityCapturingCollectorClient;
import org.deltav.collectd.identity.AgentIdentityHolder;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectorRequestBuilder;
import org.opennms.netmgt.collection.api.LocationAwareCollectorClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

/**
 * Spring-wiring integration test asserting that the {@code @Primary}
 * decorator wins bean resolution for {@link LocationAwareCollectorClient}
 * and that its {@code execute()} actually populates the
 * {@link AgentIdentityHolder}.
 *
 * <p>Stubs the horizon delegate with a simple test bean named
 * {@code locationAwareCollectorClient} (the name the decorator's
 * {@code @Qualifier} expects). Does not start the real
 * {@link org.deltav.netmgt.collectd.boot.CollectdApplication} — that live
 * scan is covered by {@code CollectdApplicationScanIT}.</p>
 */
@SpringBootTest(classes = {
        AgentIdentityWiringIT.TestApp.class
})
@TestPropertySource(properties = {
        "deltav.timeseries.enabled=true",
        "spring.main.web-application-type=none",
        "spring.main.banner-mode=off"
})
class AgentIdentityWiringIT {

    @EnableAutoConfiguration
    @Import({
            TimeseriesKafkaPublisherConfiguration.class,
            TestChannelBinderConfiguration.class
    })
    static class TestApp {

        /**
         * Stub horizon delegate registered under the default name
         * {@code locationAwareCollectorClient}. The decorator @Qualifier
         * pulls this bean in as its delegate.
         */
        @Bean(name = "locationAwareCollectorClient")
        LocationAwareCollectorClient innerClient() {
            LocationAwareCollectorClient inner = mock(LocationAwareCollectorClient.class);
            CollectorRequestBuilder builder = mock(CollectorRequestBuilder.class);
            when(inner.collect()).thenReturn(builder);
            when(builder.withAgent(org.mockito.ArgumentMatchers.any())).thenReturn(builder);
            when(builder.execute()).thenReturn(new CompletableFuture<>());
            return inner;
        }

        /**
         * {@code TimeseriesKafkaPublisherConfiguration.compositePersisterFactory}
         * injects a {@code @Qualifier("timeseriesPersisterFactory")}
         * {@link org.opennms.netmgt.collection.api.PersisterFactory}. In this
         * narrow IT we don't boot the full JPA/TSS chain that would normally
         * create it, so we provide a minimal stub.
         */
        @Bean(name = "timeseriesPersisterFactory")
        org.opennms.netmgt.collection.api.PersisterFactory innerPersisterFactory() {
            return mock(org.opennms.netmgt.collection.api.PersisterFactory.class);
        }
    }

    @Autowired
    ApplicationContext ctx;

    @Autowired
    LocationAwareCollectorClient injectedClient;

    @Autowired
    AgentIdentityHolder holder;

    @Test
    void primaryBeanIsTheCapturingDecorator() {
        assertThat(injectedClient).isInstanceOf(AgentIdentityCapturingCollectorClient.class);
    }

    @Test
    void holderBeanIsPresent() {
        assertThat(ctx.getBean(AgentIdentityHolder.class)).isSameAs(holder);
    }

    @Test
    void decoratorExecuteCapturesAgentIdentity() {
        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(123);
        when(agent.getLocationName()).thenReturn("lab-A");

        CompletableFuture<CollectionSet> future =
                injectedClient.collect().withAgent(agent).execute();

        assertThat(future).isNotNull();
        AgentIdentity captured = holder.getOrThrow();
        assertThat(captured.nodeId()).isEqualTo(123);
        assertThat(captured.location()).isEqualTo("lab-A");
        holder.clear();   // cleanup — test runs on a shared executor thread
    }
}
