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
package org.deltav.netmgt.provision.boot;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.netmgt.events.api.EventConstants;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Log;

/**
 * Decorator around the provisiond {@link EventForwarder} that increments
 * dedicated Micrometer counters for each provisiond lifecycle UEI.
 *
 * <p>Per-UEI counters (rather than one counter tagged by {@code uei}) keep
 * PromQL queries direct and avoid string-comparison work in alert rules.
 * The set of recognized UEIs is bounded; unrecognized UEIs only bump the
 * generic {@code deltav_provisiond_events_forwarded_total} bucket.
 */
public class CountingProvisionEventForwarder implements EventForwarder {

    private final EventForwarder delegate;
    private final MeterRegistry registry;

    public CountingProvisionEventForwarder(EventForwarder delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
    }

    @Override
    public void sendNow(Event event) {
        record(event);
        delegate.sendNow(event);
    }

    @Override
    public void sendNow(Log eventLog) {
        recordLog(eventLog);
        delegate.sendNow(eventLog);
    }

    @Override
    public void sendNowSync(Event event) {
        record(event);
        delegate.sendNowSync(event);
    }

    @Override
    public void sendNowSync(Log eventLog) {
        recordLog(eventLog);
        delegate.sendNowSync(eventLog);
    }

    private void recordLog(Log eventLog) {
        if (eventLog == null || eventLog.getEvents() == null
                || eventLog.getEvents().getEvent() == null) {
            return;
        }
        for (Event event : eventLog.getEvents().getEvent()) {
            record(event);
        }
    }

    private void record(Event event) {
        registry.counter(ProvisiondDomainMetrics.EVENTS_FORWARDED).increment();
        if (event == null || event.getUei() == null) {
            return;
        }
        String uei = event.getUei();
        if (EventConstants.IMPORT_STARTED_UEI.equals(uei)) {
            registry.counter(ProvisiondDomainMetrics.IMPORTS_STARTED).increment();
        } else if (EventConstants.IMPORT_SUCCESSFUL_UEI.equals(uei)) {
            registry.counter(ProvisiondDomainMetrics.IMPORTS_SUCCESSFUL).increment();
        } else if (EventConstants.IMPORT_FAILED_UEI.equals(uei)) {
            registry.counter(ProvisiondDomainMetrics.IMPORTS_FAILED).increment();
        } else if (EventConstants.NODE_ADDED_EVENT_UEI.equals(uei)) {
            registry.counter(ProvisiondDomainMetrics.NODES_ADDED).increment();
        } else if (EventConstants.NODE_UPDATED_EVENT_UEI.equals(uei)) {
            registry.counter(ProvisiondDomainMetrics.NODES_UPDATED).increment();
        } else if (EventConstants.NODE_DELETED_EVENT_UEI.equals(uei)
                || EventConstants.DUP_NODE_DELETED_EVENT_UEI.equals(uei)) {
            registry.counter(ProvisiondDomainMetrics.NODES_DELETED).increment();
        }
    }
}
