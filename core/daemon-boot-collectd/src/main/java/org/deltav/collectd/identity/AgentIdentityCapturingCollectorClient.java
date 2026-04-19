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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.opennms.netmgt.collection.api.CollectionAgent;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectorRequestBuilder;
import org.opennms.netmgt.collection.api.LocationAwareCollectorClient;
import org.opennms.netmgt.collection.api.ServiceCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spring {@code @Primary} decorator over horizon's
 * {@link LocationAwareCollectorClient}. Returns a {@link CapturingBuilder} from
 * {@link #collect()}; that builder captures the {@link CollectionAgent}'s
 * {@code nodeId} and {@code locationName} into an {@link AgentIdentityHolder}
 * just before the real RPC dispatches, so {@code TimeseriesKafkaPersister}
 * (constructed later in the same synchronous {@code doCollection()} cycle)
 * can read them.
 */
public class AgentIdentityCapturingCollectorClient implements LocationAwareCollectorClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(AgentIdentityCapturingCollectorClient.class);

    private final LocationAwareCollectorClient delegate;
    private final AgentIdentityHolder holder;

    public AgentIdentityCapturingCollectorClient(LocationAwareCollectorClient delegate,
                                                 AgentIdentityHolder holder) {
        this.delegate = delegate;
        this.holder = holder;
        LOG.info("Wrapping LocationAwareCollectorClient for identity capture; delegate = {}",
                delegate.getClass().getName());
    }

    @Override
    public CollectorRequestBuilder collect() {
        return new CapturingBuilder(delegate.collect(), holder);
    }

    /**
     * Delegating {@link CollectorRequestBuilder} that records
     * {@link AgentIdentity} into the holder at {@link #execute()} time.
     *
     * <p>Package-private so tests can construct it directly; the only
     * production construction site is {@link #collect()} on the outer class.</p>
     */
    static final class CapturingBuilder implements CollectorRequestBuilder {

        private final CollectorRequestBuilder delegate;
        private final AgentIdentityHolder holder;
        private CollectionAgent agent;

        CapturingBuilder(CollectorRequestBuilder delegate, AgentIdentityHolder holder) {
            this.delegate = delegate;
            this.holder = holder;
        }

        @Override
        public CollectorRequestBuilder withAgent(CollectionAgent agent) {
            this.agent = agent;
            delegate.withAgent(agent);
            return this;
        }

        @Override
        public CollectorRequestBuilder withSystemId(String systemId) {
            delegate.withSystemId(systemId);
            return this;
        }

        @Override
        public CollectorRequestBuilder withCollector(ServiceCollector collector) {
            delegate.withCollector(collector);
            return this;
        }

        @Override
        public CollectorRequestBuilder withCollectorClassName(String className) {
            delegate.withCollectorClassName(className);
            return this;
        }

        @Override
        public CollectorRequestBuilder withTimeToLive(Long ttlInMs) {
            delegate.withTimeToLive(ttlInMs);
            return this;
        }

        @Override
        public CollectorRequestBuilder withAttribute(String key, Object value) {
            delegate.withAttribute(key, value);
            return this;
        }

        @Override
        public CollectorRequestBuilder withAttributes(Map<String, Object> attributes) {
            delegate.withAttributes(attributes);
            return this;
        }

        @Override
        public CompletableFuture<CollectionSet> execute() {
            if (agent != null) {
                holder.set(agent.getNodeId(), agent.getLocationName());
            }
            return delegate.execute();
        }
    }
}
