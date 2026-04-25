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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.collectd.identity.AgentIdentityHolder;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration.FanoutPersister;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration.FanoutPersisterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collection.api.ServiceParameters;

class FanoutPersisterTest {

    @Test
    void completeCollectionSetCallsInnerThenKafkaInOrder() {
        Persister inner = mock(Persister.class);
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(1, "Default");
        TimeseriesKafkaPersister kafka = new TimeseriesKafkaPersister(publisher, "default", holder, new SimpleMeterRegistry());
        FanoutPersister fanout = new FanoutPersister(inner, kafka, new SimpleMeterRegistry(), false, false);

        CollectionSet set = mock(CollectionSet.class);
        fanout.visitCollectionSet(set);
        fanout.completeCollectionSet(set);

        InOrder order = inOrder(inner, publisher);
        order.verify(inner).visitCollectionSet(set);
        order.verify(inner).completeCollectionSet(set);
        order.verify(publisher).publish(any(), any(), any(Integer.class), any());
    }

    @Test
    void innerPersisterThrowingRuntimeExceptionDoesNotBlockKafkaDelegate() {
        // Per-delegate isolation contract: a RuntimeException from the inner
        // persister must not prevent the Kafka publish for the same poll.
        // Horizon's TimeseriesPersister has surfaced several pre-existing
        // delta-v failure modes (transaction rollback-only, NPE in
        // TimeseriesPersistOperationBuilder, etc.); the Kafka path must
        // survive all of them so consumers still receive the protobuf record.
        Persister inner = mock(Persister.class);
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(1, "Default");
        TimeseriesKafkaPersister kafka = new TimeseriesKafkaPersister(publisher, "default", holder, new SimpleMeterRegistry());
        FanoutPersister fanout = new FanoutPersister(inner, kafka, new SimpleMeterRegistry(), false, false);

        CollectionSet set = mock(CollectionSet.class);
        doThrow(new RuntimeException("inner boom"))
                .when(inner).completeCollectionSet(set);

        fanout.visitCollectionSet(set);
        fanout.completeCollectionSet(set);  // must not throw

        verify(publisher).publish(any(), any(), any(Integer.class), any());
    }

    @Test
    void innerPersisterThrowingLinkageErrorDoesNotBlockKafkaDelegate() {
        // The catch widens to Throwable (not just RuntimeException) because
        // delta-v Collectd has surfaced NoSuchMethodError (LinkageError) on
        // classpath-mismatched horizon jars (ResourceTypeUtils). Errors from
        // the inner persister must not kill the Kafka path either.
        Persister inner = mock(Persister.class);
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(1, "Default");
        TimeseriesKafkaPersister kafka = new TimeseriesKafkaPersister(publisher, "default", holder, new SimpleMeterRegistry());
        FanoutPersister fanout = new FanoutPersister(inner, kafka, new SimpleMeterRegistry(), false, false);

        CollectionSet set = mock(CollectionSet.class);
        doThrow(new NoSuchMethodError("simulated classpath mismatch"))
                .when(inner).visitCollectionSet(set);

        fanout.visitCollectionSet(set);  // must not propagate
        fanout.completeCollectionSet(set);

        verify(publisher).publish(any(), any(), any(Integer.class), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void factoryCreatesFanoutWithIndependentKafkaPersisterPerCall() {
        PersisterFactory innerFactory = mock(PersisterFactory.class);
        Persister innerPersister1 = mock(Persister.class);
        Persister innerPersister2 = mock(Persister.class);
        when(innerFactory.createPersister(any(), any()))
                .thenReturn(innerPersister1, innerPersister2);

        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        FanoutPersisterFactory factory = new FanoutPersisterFactory(innerFactory, publisher, holder,
                new SimpleMeterRegistry(), false, false);

        ServiceParameters paramsA = mock(ServiceParameters.class);
        Map<String, Object> mapA = new HashMap<>();
        mapA.put("collection", "pkg-A");
        when(paramsA.getParameters()).thenReturn((Map) mapA);

        ServiceParameters paramsB = mock(ServiceParameters.class);
        Map<String, Object> mapB = new HashMap<>();
        mapB.put("collection", "pkg-B");
        when(paramsB.getParameters()).thenReturn((Map) mapB);

        Persister pA = factory.createPersister(paramsA, null);
        Persister pB = factory.createPersister(paramsB, null);

        // Both are distinct FanoutPersister instances wrapping different inner persisters
        // and different TimeseriesKafkaPersister instances (different package).
        CollectionSet setA = mock(CollectionSet.class);
        holder.set(1, "");
        pA.visitCollectionSet(setA);
        pA.completeCollectionSet(setA);

        verify(innerPersister1).completeCollectionSet(setA);
        verify(innerPersister2, never()).completeCollectionSet(any());
        verify(publisher).publish(setA, "pkg-A", 1, "");
    }

    // --- Task 3 observability + fail-fast behavioural tests ------------------
    // Pin the counter-increment and fail-fast behaviour added in Task 3.
    // These mock TimeseriesKafkaPersister directly (unlike the older tests,
    // which wire a real TimeseriesKafkaPersister around a mock publisher).

    private MeterRegistry registry;
    private Persister innerV2;
    private TimeseriesKafkaPersister kafkaV2;

    @BeforeEach
    void setUpV2() {
        registry = new SimpleMeterRegistry();
        innerV2 = mock(Persister.class);
        kafkaV2 = mock(TimeseriesKafkaPersister.class);
    }

    private FanoutPersister makeV2(boolean failFastInner, boolean failFastKafka) {
        return new FanoutPersister(innerV2, kafkaV2, registry, failFastInner, failFastKafka);
    }

    @Test
    void counter_increments_on_inner_failure_in_visitResource() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new RuntimeException("inner boom")).when(innerV2).visitResource(r);
        doNothing().when(kafkaV2).visitResource(r);

        makeV2(false, false).visitResource(r);

        Counter c = registry.find("deltav.collectd.persister.inner.failures").tag("step", "visitResource").counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
        verify(kafkaV2).visitResource(r);
    }

    @Test
    void counter_increments_on_kafka_failure_in_visitResource() {
        CollectionResource r = mock(CollectionResource.class);
        doNothing().when(innerV2).visitResource(r);
        doThrow(new RuntimeException("kafka boom")).when(kafkaV2).visitResource(r);

        makeV2(false, false).visitResource(r);

        Counter c = registry.find("deltav.collectd.persister.kafka.failures").tag("step", "visitResource").counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
        verify(innerV2).visitResource(r);
    }

    @Test
    void counters_preregistered_at_startup_with_zero_value() {
        makeV2(false, false);
        String[] steps = new String[]{
                "visitCollectionSet", "visitResource", "visitGroup", "visitAttribute",
                "completeAttribute", "completeGroup", "completeResource", "completeCollectionSet",
                "persistNumericAttribute", "persistStringAttribute"
        };
        for (String step : steps) {
            Counter ic = registry.find("deltav.collectd.persister.inner.failures").tag("step", step).counter();
            Counter kc = registry.find("deltav.collectd.persister.kafka.failures").tag("step", step).counter();
            assertThat(ic).as("inner counter step=%s", step).isNotNull();
            assertThat(ic.count()).isZero();
            assertThat(kc).as("kafka counter step=%s", step).isNotNull();
            assertThat(kc.count()).isZero();
        }
    }

    @Test
    void fail_fast_inner_true_rethrows() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new RuntimeException("inner boom")).when(innerV2).visitResource(r);

        FanoutPersister p = makeV2(true, false);
        assertThatThrownBy(() -> p.visitResource(r))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("fail-fast enabled");
    }

    @Test
    void fail_fast_inner_false_swallows_and_continues() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new RuntimeException("inner boom")).when(innerV2).visitResource(r);
        doNothing().when(kafkaV2).visitResource(r);

        makeV2(false, false).visitResource(r);  // must NOT throw

        verify(kafkaV2).visitResource(r);
    }

    @Test
    void fail_fast_kafka_true_rethrows() {
        CollectionResource r = mock(CollectionResource.class);
        doNothing().when(innerV2).visitResource(r);
        doThrow(new RuntimeException("kafka boom")).when(kafkaV2).visitResource(r);

        FanoutPersister p = makeV2(false, true);
        assertThatThrownBy(() -> p.visitResource(r))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("fail-fast enabled");
    }

    @Test
    void throwable_not_just_runtime_exception() {
        CollectionResource r = mock(CollectionResource.class);
        doThrow(new LinkageError("NoSuchMethodError from horizon"))
                .when(innerV2).visitResource(r);
        doNothing().when(kafkaV2).visitResource(r);

        makeV2(false, false).visitResource(r);  // must NOT throw

        Counter c = registry.find("deltav.collectd.persister.inner.failures").tag("step", "visitResource").counter();
        assertThat(c.count()).isEqualTo(1.0);
        verify(kafkaV2).visitResource(r);
    }
}
