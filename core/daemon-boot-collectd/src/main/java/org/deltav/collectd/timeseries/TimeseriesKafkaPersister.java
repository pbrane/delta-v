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

import org.deltav.collectd.identity.AgentIdentity;
import org.deltav.collectd.identity.AgentIdentityHolder;
import org.opennms.netmgt.collection.api.AttributeGroup;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;

/**
 * Thin horizon {@link Persister} adapter that hands every {@link CollectionSet}
 * to the shared {@link TimeseriesKafkaPublisher} once, at
 * {@link #completeCollectionSet(CollectionSet)}. All per-attribute callbacks
 * are no-ops — the translator does its own walk.
 *
 * <p>Identity ({@code nodeId} + {@code location}) is captured by
 * {@code AgentIdentityCapturingCollectorClient} before each RPC dispatch and
 * read from the injected {@link AgentIdentityHolder} at publish time. If the
 * holder is empty (decorator not wired) or {@code nodeId <= 0}, this persister
 * throws {@link IllegalStateException}; {@code FanoutPersister} catches it,
 * increments {@code deltav.collectd.persister.kafka.failures{step=completeCollectionSet}},
 * and logs WARN. The inner persister path is unaffected.</p>
 *
 * <p>{@code collectionPackage} is extracted once by
 * {@code FanoutPersisterFactory.createPersister} from the horizon
 * {@code ServiceParameters} and passed as an explicit constructor argument.</p>
 */
public class TimeseriesKafkaPersister implements Persister {

    private final TimeseriesKafkaPublisher publisher;
    private final String collectionPackage;
    private final AgentIdentityHolder holder;
    private CollectionSet capturedSet;

    public TimeseriesKafkaPersister(TimeseriesKafkaPublisher publisher,
                                     String collectionPackage,
                                     AgentIdentityHolder holder) {
        this.publisher = publisher;
        this.collectionPackage = collectionPackage;
        this.holder = holder;
    }

    @Override
    public void visitCollectionSet(CollectionSet set) {
        this.capturedSet = set;
    }

    @Override
    public void visitResource(CollectionResource resource) { /* no-op */ }

    @Override
    public void visitGroup(AttributeGroup group) { /* no-op */ }

    @Override
    public void visitAttribute(CollectionAttribute attribute) { /* no-op */ }

    @Override
    public void completeAttribute(CollectionAttribute attribute) { /* no-op */ }

    @Override
    public void completeGroup(AttributeGroup group) { /* no-op */ }

    @Override
    public void completeResource(CollectionResource resource) { /* no-op */ }

    @Override
    public void completeCollectionSet(CollectionSet set) {
        try {
            if (capturedSet != null) {
                AgentIdentity identity = holder.getOrThrow();
                if (identity.nodeId() <= 0) {
                    throw new IllegalStateException(
                            "Invalid agent identity for Kafka publish: nodeId must be > 0, got "
                                    + identity.nodeId());
                }
                publisher.publish(capturedSet, collectionPackage,
                        identity.nodeId(), identity.location());
            }
        } finally {
            holder.clear();
            capturedSet = null;
        }
    }

    @Override
    public void persistNumericAttribute(CollectionAttribute attribute) { /* no-op */ }

    @Override
    public void persistStringAttribute(CollectionAttribute attribute) { /* no-op */ }
}
