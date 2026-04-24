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

/**
 * Canonical Micrometer meter names for eventtranslator domain signals.
 * Names become {@code deltav_eventtranslator_*} at
 * {@code /actuator/prometheus} after Micrometer's dot-to-underscore flattening.
 */
public final class EventTranslatorDomainMetrics {

    private EventTranslatorDomainMetrics() {}

    /** Counter: events received by the translator's {@code onEvent} listener. */
    public static final String EVENTS_RECEIVED = "deltav.eventtranslator.events.received";

    /** Timer: wall-clock duration of each {@code onEvent} invocation. */
    public static final String EVENT_PROCESSING_DURATION = "deltav.eventtranslator.event.processing.duration";
}
