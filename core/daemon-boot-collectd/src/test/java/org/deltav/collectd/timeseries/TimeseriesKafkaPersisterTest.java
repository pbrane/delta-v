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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.deltav.collectd.identity.AgentIdentityHolder;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.AttributeGroup;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;

class TimeseriesKafkaPersisterTest {

    @Test
    void populatedHolderCausesPublishWithCapturedIdentityAndClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(42, "Site-A");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "critical-infra", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);
        persister.completeCollectionSet(set);

        verify(publisher).publish(eq(set), eq("critical-infra"), eq(42), eq("Site-A"));
        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void emptyHolderThrowsIllegalStateExceptionButStillClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);

        assertThatThrownBy(() -> persister.completeCollectionSet(set))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("AgentIdentity not populated");
        verifyNoInteractions(publisher);
        // idempotent clear — must not throw on already-empty slot:
        holder.clear();
    }

    @Test
    void holderWithZeroNodeIdThrowsAndClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(0, "Default");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);

        assertThatThrownBy(() -> persister.completeCollectionSet(set))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nodeId must be > 0, got 0");
        verifyNoInteractions(publisher);
        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void holderWithNegativeNodeIdThrowsWithActualValueInMessage() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(-5, "Default");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.visitCollectionSet(set);

        assertThatThrownBy(() -> persister.completeCollectionSet(set))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("got -5");
    }

    @Test
    void completeCollectionSetWithoutVisitDoesNothingButStillClearsHolder() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        holder.set(7, "X");
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        CollectionSet set = mock(CollectionSet.class);
        persister.completeCollectionSet(set);

        verifyNoInteractions(publisher);
        assertThatThrownBy(holder::getOrThrow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void visitResourceVisitGroupVisitAttributeAreNoOps() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        persister.visitResource(mock(CollectionResource.class));
        persister.visitGroup(mock(AttributeGroup.class));
        persister.visitAttribute(mock(CollectionAttribute.class));
        persister.completeAttribute(mock(CollectionAttribute.class));
        persister.completeGroup(mock(AttributeGroup.class));
        persister.completeResource(mock(CollectionResource.class));

        verifyNoInteractions(publisher);
    }

    @Test
    void persistNumericAttributeAndPersistStringAttributeAreNoOps() {
        TimeseriesKafkaPublisher publisher = mock(TimeseriesKafkaPublisher.class);
        AgentIdentityHolder holder = new AgentIdentityHolder();
        TimeseriesKafkaPersister persister =
                new TimeseriesKafkaPersister(publisher, "default", holder);

        persister.persistNumericAttribute(mock(CollectionAttribute.class));
        persister.persistStringAttribute(mock(CollectionAttribute.class));

        verifyNoInteractions(publisher);
    }
}
