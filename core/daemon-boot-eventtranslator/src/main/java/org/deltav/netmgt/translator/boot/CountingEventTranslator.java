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
package org.deltav.netmgt.translator.boot;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.opennms.netmgt.events.api.model.IEvent;
import org.opennms.netmgt.translator.EventTranslator;

/**
 * {@link EventTranslator} subclass that increments Micrometer counters on
 * every event received via the horizon {@code EventListener} contract.
 * Delegates to {@code super.onEvent} so translation semantics are unchanged.
 */
public class CountingEventTranslator extends EventTranslator {

    private final Counter eventsReceived;
    private final Timer processingDuration;

    public CountingEventTranslator(MeterRegistry registry) {
        super();
        this.eventsReceived = registry.counter(EventTranslatorDomainMetrics.EVENTS_RECEIVED);
        this.processingDuration = registry.timer(EventTranslatorDomainMetrics.EVENT_PROCESSING_DURATION);
    }

    @Override
    public void onEvent(IEvent event) {
        eventsReceived.increment();
        processingDuration.record(() -> super.onEvent(event));
    }
}
