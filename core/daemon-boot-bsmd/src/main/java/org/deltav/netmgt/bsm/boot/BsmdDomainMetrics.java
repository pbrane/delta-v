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
package org.deltav.netmgt.bsm.boot;

/**
 * Canonical Micrometer meter names for bsmd domain signals. Names become
 * {@code deltav_bsmd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 */
public final class BsmdDomainMetrics {

    private BsmdDomainMetrics() {}

    /** Counter: alarm-provider reduction-key lookup invocations. */
    public static final String ALARM_LOOKUPS               = "deltav.bsmd.alarm.lookups";

    /** Timer: alarm-provider lookup wall-clock duration. */
    public static final String ALARM_LOOKUP_DURATION       = "deltav.bsmd.alarm.lookup.duration";

    /** Counter: number of reduction keys requested (sum across lookups). */
    public static final String ALARM_LOOKUP_KEYS_REQUESTED = "deltav.bsmd.alarm.lookup.keys.requested";

    /** Counter: number of reduction keys resolved (sum across lookups). */
    public static final String ALARM_LOOKUP_KEYS_MATCHED   = "deltav.bsmd.alarm.lookup.keys.matched";
}
