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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration.FanoutPersister;
import org.deltav.collectd.timeseries.TimeseriesKafkaPublisherConfiguration.FanoutPersisterFactory;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collection.api.ServiceParameters;

class FanoutPersisterTest {

    @Test
    void completeCollectionSetCallsInnerThenKafkaInOrder() {
        Persister inner = mock(Persister.class);
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        ServiceParameters sp = mock(ServiceParameters.class);
        when(sp.getParameters()).thenReturn(new HashMap<>());
        TimeseriesKafkaPersister kafka = new TimeseriesKafkaPersister(publisher, sp);
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
        ServiceParameters sp = mock(ServiceParameters.class);
        when(sp.getParameters()).thenReturn(new HashMap<>());
        TimeseriesKafkaPersister kafka = new TimeseriesKafkaPersister(publisher, sp);
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
        ServiceParameters sp = mock(ServiceParameters.class);
        when(sp.getParameters()).thenReturn(new HashMap<>());
        TimeseriesKafkaPersister kafka = new TimeseriesKafkaPersister(publisher, sp);
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
        FanoutPersisterFactory factory = new FanoutPersisterFactory(innerFactory, publisher, new SimpleMeterRegistry(), false, false);

        ServiceParameters paramsA = mock(ServiceParameters.class);
        Map<String, Object> mapA = new HashMap<>();
        mapA.put("collection", "pkg-A");
        mapA.put("node-id", "1");
        when(paramsA.getParameters()).thenReturn((Map) mapA);

        ServiceParameters paramsB = mock(ServiceParameters.class);
        Map<String, Object> mapB = new HashMap<>();
        mapB.put("collection", "pkg-B");
        mapB.put("node-id", "2");
        when(paramsB.getParameters()).thenReturn((Map) mapB);

        Persister pA = factory.createPersister(paramsA, null);
        Persister pB = factory.createPersister(paramsB, null);

        // Both are distinct FanoutPersister instances wrapping different inner persisters
        // and different TimeseriesKafkaPersister instances (different nodeId/package).
        CollectionSet setA = mock(CollectionSet.class);
        pA.visitCollectionSet(setA);
        pA.completeCollectionSet(setA);

        verify(innerPersister1).completeCollectionSet(setA);
        verify(innerPersister2, never()).completeCollectionSet(any());
        verify(publisher).publish(setA, "pkg-A", 1, "");
    }
}
