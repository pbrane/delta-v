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
package org.deltav.netmgt.trapd.boot;

import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.netmgt.config.TrapdConfig;
import org.opennms.netmgt.config.api.EventConfDao;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.trapd.TrapDTO;
import org.opennms.netmgt.trapd.TrapLogDTO;
import org.opennms.netmgt.trapd.TrapSinkConsumer;

/**
 * {@link TrapSinkConsumer} subclass that increments Micrometer counters and
 * records timing on every batch received via the Sink topic from Minion.
 * Delegates to {@code super.handleMessage} so trap processing semantics
 * are unchanged.
 *
 * <p>Counters are pre-resolved against the registry once per location/unlabeled
 * pair; the Timer is unlabeled.
 */
public class CountingTrapSinkConsumer extends TrapSinkConsumer {

    private final MeterRegistry registry;
    private final Timer handleDuration;

    public CountingTrapSinkConsumer(MessageConsumerManager messageConsumerManager,
                                    EventConfDao eventConfDao,
                                    EventForwarder eventForwarder,
                                    InterfaceToNodeCache interfaceToNodeCache,
                                    TrapdConfig config,
                                    DistPollerDao distPollerDao,
                                    MeterRegistry registry) {
        super(messageConsumerManager, eventConfDao, eventForwarder, interfaceToNodeCache,
                config, distPollerDao);
        this.registry = registry;
        this.handleDuration = registry.timer(TrapdDomainMetrics.HANDLE_DURATION);
    }

    @Override
    public void handleMessage(TrapLogDTO message) {
        String location = location(message);
        registry.counter(TrapdDomainMetrics.TRAPLOGS_RECEIVED,
                TrapdDomainMetrics.TAG_LOCATION, location).increment();
        List<TrapDTO> batch = message != null ? message.getMessages() : null;
        int size = batch != null ? batch.size() : 0;
        if (size > 0) {
            registry.counter(TrapdDomainMetrics.TRAPS_IN_BATCH,
                    TrapdDomainMetrics.TAG_LOCATION, location).increment(size);
        }
        handleDuration.record(() -> super.handleMessage(message));
    }

    private static String location(TrapLogDTO message) {
        if (message == null) return "unknown";
        String l = message.getLocation();
        return l != null ? l : "unknown";
    }
}
