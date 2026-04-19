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
package org.deltav.collectd.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectorRequestBuilder;
import org.opennms.netmgt.collection.api.ServiceCollector;

class AgentIdentityCapturingCollectorRequestBuilderTest {

    @Test
    void withAgentThenExecuteSetsHolderAndDelegates() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        CompletableFuture<CollectionSet> future = new CompletableFuture<>();
        when(delegate.execute()).thenReturn(future);
        when(delegate.withAgent(any())).thenReturn(delegate);

        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(42);
        when(agent.getLocationName()).thenReturn("Site-A");

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        CompletableFuture<CollectionSet> result = builder.withAgent(agent).execute();

        assertThat(result).isSameAs(future);
        assertThat(holder.getOrThrow().nodeId()).isEqualTo(42);
        assertThat(holder.getOrThrow().location()).isEqualTo("Site-A");
        verify(delegate).withAgent(agent);
        verify(delegate).execute();
    }

    @Test
    void executeWithoutAgentDoesNotSetHolderButStillDelegates() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.execute();

        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
        verify(delegate).execute();
    }

    @Test
    void agentWithZeroNodeIdCapturedAsIs() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.withAgent(any())).thenReturn(delegate);
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(0);
        when(agent.getLocationName()).thenReturn("Default");

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.withAgent(agent).execute();

        assertThat(holder.getOrThrow().nodeId()).isZero();
        assertThat(holder.getOrThrow().location()).isEqualTo("Default");
    }

    @Test
    void agentWithNullLocationCapturedAsEmptyString() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.withAgent(any())).thenReturn(delegate);
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        CollectionAgent agent = mock(CollectionAgent.class);
        when(agent.getNodeId()).thenReturn(5);
        when(agent.getLocationName()).thenReturn(null);

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.withAgent(agent).execute();

        assertThat(holder.getOrThrow().nodeId()).isEqualTo(5);
        assertThat(holder.getOrThrow().location()).isEqualTo("");
    }

    @Test
    void withXxxMethodsReturnThisAndDelegate() {
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        when(delegate.withSystemId("sys")).thenReturn(delegate);
        when(delegate.withCollector(any())).thenReturn(delegate);
        when(delegate.withCollectorClassName("class")).thenReturn(delegate);
        when(delegate.withTimeToLive(1000L)).thenReturn(delegate);
        when(delegate.withAttribute("k", "v")).thenReturn(delegate);
        when(delegate.withAttributes(Map.of("a", "b"))).thenReturn(delegate);

        ServiceCollector svc = mock(ServiceCollector.class);
        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);

        assertThat(builder.withSystemId("sys")).isSameAs(builder);
        assertThat(builder.withCollector(svc)).isSameAs(builder);
        assertThat(builder.withCollectorClassName("class")).isSameAs(builder);
        assertThat(builder.withTimeToLive(1000L)).isSameAs(builder);
        assertThat(builder.withAttribute("k", "v")).isSameAs(builder);
        assertThat(builder.withAttributes(Map.of("a", "b"))).isSameAs(builder);

        verify(delegate).withSystemId("sys");
        verify(delegate).withCollector(svc);
        verify(delegate).withCollectorClassName("class");
        verify(delegate).withTimeToLive(1000L);
        verify(delegate).withAttribute("k", "v");
        verify(delegate).withAttributes(Map.of("a", "b"));
    }

    @Test
    void executeDoesNotInteractWithHolderIfAgentNeverSet() {
        // Verifies the no-agent path uses no ThreadLocal state at all.
        CollectorRequestBuilder delegate = mock(CollectorRequestBuilder.class);
        AgentIdentityHolder holder = mock(AgentIdentityHolder.class);
        when(delegate.execute()).thenReturn(new CompletableFuture<>());

        AgentIdentityCapturingCollectorClient.CapturingBuilder builder =
                new AgentIdentityCapturingCollectorClient.CapturingBuilder(delegate, holder);
        builder.execute();

        verifyNoInteractions(holder);
    }
}
