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

/**
 * Canonical Micrometer meter names for discovery domain signals. Names become
 * {@code deltav_discovery_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>{@code TASKS_HANDLED} fires once per {@code handleDiscoveryTask}
 * invocation — i.e., once per scheduled discovery run. {@code SUSPECTS_FOUND}
 * is incremented from a wrapping {@link org.opennms.netmgt.events.api.EventForwarder}
 * each time a {@code uei.opennms.org/internal/discovery/newSuspect} event is
 * forwarded — this is the signal an HPA actually wants ("discovery is finding
 * things").
 */
public final class DiscoveryDomainMetrics {

    private DiscoveryDomainMetrics() {}

    public static final String TASKS_HANDLED      = "deltav.discovery.tasks.handled";
    public static final String HANDLE_DURATION    = "deltav.discovery.handle.duration";
    public static final String SUSPECTS_FOUND     = "deltav.discovery.suspects.found";
    public static final String EVENTS_FORWARDED   = "deltav.discovery.events.forwarded";
}
