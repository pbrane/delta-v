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
package org.deltav.core.daemon.common;

import java.util.Collection;
import java.util.Objects;

import org.deltav.core.daemon.common.EventConfEnrichmentService;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.events.api.EventListener;
import org.opennms.netmgt.events.api.EventProxyException;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Log;

/**
 * Delegating {@link EventIpcManager} wrapper that enriches events with
 * alarm-data, severity, logmsg, and descr from the event configuration
 * before forwarding them to the real {@link EventIpcManager}.
 *
 * <p>Wired as the primary {@link EventIpcManager} by
 * {@link KafkaEventTransportConfiguration} so that ALL events from ALL
 * daemons pass through {@link EventConfEnrichmentService#enrichEvent}
 * before reaching Kafka. This replaces the role of Eventd's EventExpander
 * in classic OpenNMS.</p>
 */
public class EventIpcManagerEnrichingWrapper implements EventIpcManager {

    private final EventIpcManager delegate;
    private final EventConfEnrichmentService enrichmentService;

    public EventIpcManagerEnrichingWrapper(EventIpcManager delegate,
                                           EventConfEnrichmentService enrichmentService) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.enrichmentService = Objects.requireNonNull(enrichmentService, "enrichmentService");
    }

    @Override
    public void sendNow(Event event) {
        enrichmentService.enrichEvent(event);
        delegate.sendNow(event);
    }

    @Override
    public void sendNow(Log eventLog) {
        if (eventLog != null && eventLog.getEvents() != null) {
            for (Event event : eventLog.getEvents().getEventCollection()) {
                enrichmentService.enrichEvent(event);
            }
        }
        delegate.sendNow(eventLog);
    }

    @Override
    public void sendNowSync(Event event) {
        enrichmentService.enrichEvent(event);
        delegate.sendNowSync(event);
    }

    @Override
    public void sendNowSync(Log eventLog) {
        if (eventLog != null && eventLog.getEvents() != null) {
            for (Event event : eventLog.getEvents().getEventCollection()) {
                enrichmentService.enrichEvent(event);
            }
        }
        delegate.sendNowSync(eventLog);
    }

    @Override
    public void send(Event event) throws EventProxyException {
        enrichmentService.enrichEvent(event);
        delegate.send(event);
    }

    @Override
    public void send(Log eventLog) throws EventProxyException {
        if (eventLog != null && eventLog.getEvents() != null) {
            for (Event event : eventLog.getEvents().getEventCollection()) {
                enrichmentService.enrichEvent(event);
            }
        }
        delegate.send(eventLog);
    }

    @Override
    public void addEventListener(EventListener listener) {
        delegate.addEventListener(listener);
    }

    @Override
    public void addEventListener(EventListener listener, Collection<String> ueis) {
        delegate.addEventListener(listener, ueis);
    }

    @Override
    public void addEventListener(EventListener listener, String uei) {
        delegate.addEventListener(listener, uei);
    }

    @Override
    public void removeEventListener(EventListener listener) {
        delegate.removeEventListener(listener);
    }

    @Override
    public void removeEventListener(EventListener listener, Collection<String> ueis) {
        delegate.removeEventListener(listener, ueis);
    }

    @Override
    public void removeEventListener(EventListener listener, String uei) {
        delegate.removeEventListener(listener, uei);
    }

    @Override
    public boolean hasEventListener(String uei) {
        return delegate.hasEventListener(uei);
    }
}
