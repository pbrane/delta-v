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
package org.deltav.netmgt.enlinkd.boot;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.netmgt.topologies.service.api.OnmsTopology;
import org.opennms.netmgt.topologies.service.api.OnmsTopologyConsumer;
import org.opennms.netmgt.topologies.service.api.OnmsTopologyDao;
import org.opennms.netmgt.topologies.service.api.OnmsTopologyMessage;
import org.opennms.netmgt.topologies.service.api.OnmsTopologyProtocol;
import org.opennms.netmgt.topologies.service.api.OnmsTopologyUpdater;

/**
 * Decorator around {@link OnmsTopologyDao} that increments Micrometer counters
 * on every topology message and tracks the number of currently registered
 * updaters as a gauge.
 *
 * <p>Tags on the per-update counter:
 * <ul>
 *   <li>{@code protocol} — bounded to the {@code ProtocolSupported} enum
 *       (NODES, CDP, LLDP, ISIS, OSPF, OSPFAREA, BRIDGE, NETWORKROUTER,
 *       USERDEFINED) plus any that future horizon adds.</li>
 *   <li>{@code status} — bounded to {@code TopologyMessageStatus}
 *       (UPDATE, DELETE).</li>
 * </ul>
 */
public class CountingOnmsTopologyDao implements OnmsTopologyDao {

    private final OnmsTopologyDao delegate;
    private final MeterRegistry registry;
    private final AtomicLong activeUpdaters = new AtomicLong(0);

    public CountingOnmsTopologyDao(OnmsTopologyDao delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
        Gauge.builder(EnlinkdDomainMetrics.TOPOLOGY_UPDATERS_ACTIVE, activeUpdaters, AtomicLong::get)
                .register(registry);
    }

    @Override
    public OnmsTopology getTopology(String id) {
        return delegate.getTopology(id);
    }

    @Override
    public Map<OnmsTopologyProtocol, OnmsTopology> getTopologies() {
        return delegate.getTopologies();
    }

    @Override
    public Set<OnmsTopologyProtocol> getSupportedProtocols() {
        return delegate.getSupportedProtocols();
    }

    @Override
    public void register(OnmsTopologyUpdater updater) {
        delegate.register(updater);
        activeUpdaters.incrementAndGet();
    }

    @Override
    public void unregister(OnmsTopologyUpdater updater) {
        delegate.unregister(updater);
        activeUpdaters.updateAndGet(v -> Math.max(0L, v - 1));
    }

    @Override
    public void subscribe(OnmsTopologyConsumer consumer) {
        delegate.subscribe(consumer);
    }

    @Override
    public void unsubscribe(OnmsTopologyConsumer consumer) {
        delegate.unsubscribe(consumer);
    }

    @Override
    public void update(OnmsTopologyUpdater updater, OnmsTopologyMessage message) {
        delegate.update(updater, message);
        String protocol = message != null && message.getProtocol() != null
                ? message.getProtocol().getId() : "unknown";
        String status = message != null && message.getMessagestatus() != null
                ? message.getMessagestatus().name() : "unknown";
        registry.counter(EnlinkdDomainMetrics.TOPOLOGY_UPDATES,
                EnlinkdDomainMetrics.TAG_PROTOCOL, protocol,
                EnlinkdDomainMetrics.TAG_STATUS, status).increment();
    }
}
