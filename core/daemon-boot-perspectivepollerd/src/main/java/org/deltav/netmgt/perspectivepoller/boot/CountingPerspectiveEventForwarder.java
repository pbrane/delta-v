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
package org.deltav.netmgt.perspectivepoller.boot;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.netmgt.events.api.EventConstants;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Log;

/**
 * Decorator around the perspectivepollerd {@link EventForwarder} that
 * increments dedicated Micrometer counters for perspective lifecycle UEIs.
 *
 * <p>PerspectivePollerd's constructor parameter 9 is plain
 * {@code EventForwarder} (not {@code EventIpcManager}), so this wrapper
 * only needs to satisfy the four-method {@link EventForwarder} interface.
 * The unwrapped {@link org.opennms.netmgt.events.api.EventIpcManager} is
 * still used by the {@code AnnotationBasedEventListenerAdapter} beans
 * for inbound event-listener registration.
 */
public class CountingPerspectiveEventForwarder implements EventForwarder {

    private final EventForwarder delegate;
    private final MeterRegistry registry;

    public CountingPerspectiveEventForwarder(EventForwarder delegate, MeterRegistry registry) {
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
        registry.counter(PerspectivePollerdDomainMetrics.EVENTS_FORWARDED).increment();
        if (event == null || event.getUei() == null) {
            return;
        }
        String uei = event.getUei();
        if (EventConstants.PERSPECTIVE_NODE_LOST_SERVICE_UEI.equals(uei)) {
            registry.counter(PerspectivePollerdDomainMetrics.SERVICE_LOST_TOTAL).increment();
        } else if (EventConstants.PERSPECTIVE_NODE_REGAINED_SERVICE_UEI.equals(uei)) {
            registry.counter(PerspectivePollerdDomainMetrics.SERVICE_REGAINED_TOTAL).increment();
        }
    }
}
