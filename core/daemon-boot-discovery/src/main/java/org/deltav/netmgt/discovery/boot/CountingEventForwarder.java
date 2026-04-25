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
package org.deltav.netmgt.discovery.boot;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.netmgt.events.api.EventConstants;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Log;

/**
 * Decorator around the discovery daemon's {@link EventForwarder} that
 * increments {@code deltav_discovery_events_forwarded_total} for every event
 * the daemon publishes, and a separate {@code _suspects_found_total} for
 * the {@code newSuspect} UEI specifically.
 *
 * <p>Operationally, suspects-found is the signal that discovery actually
 * accomplished work — it's the metric an HPA wants to scale by. Total events
 * is a coarser denominator (covers internal Discovery lifecycle UEIs, not
 * just suspects).
 */
public class CountingEventForwarder implements EventForwarder {

    private final EventForwarder delegate;
    private final MeterRegistry registry;

    public CountingEventForwarder(EventForwarder delegate, MeterRegistry registry) {
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
        if (eventLog != null && eventLog.getEvents() != null && eventLog.getEvents().getEvent() != null) {
            for (Event event : eventLog.getEvents().getEvent()) {
                record(event);
            }
        }
        delegate.sendNow(eventLog);
    }

    @Override
    public void sendNowSync(Event event) {
        record(event);
        delegate.sendNowSync(event);
    }

    @Override
    public void sendNowSync(Log eventLog) {
        if (eventLog != null && eventLog.getEvents() != null && eventLog.getEvents().getEvent() != null) {
            for (Event event : eventLog.getEvents().getEvent()) {
                record(event);
            }
        }
        delegate.sendNowSync(eventLog);
    }

    private void record(Event event) {
        registry.counter(DiscoveryDomainMetrics.EVENTS_FORWARDED).increment();
        if (event != null && EventConstants.NEW_SUSPECT_INTERFACE_EVENT_UEI.equals(event.getUei())) {
            registry.counter(DiscoveryDomainMetrics.SUSPECTS_FOUND).increment();
        }
    }
}
