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
package org.deltav.netmgt.alarmd.boot;

/**
 * Canonical Micrometer meter names for alarmd domain signals. Names become
 * {@code deltav_alarmd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>All counters are tagged by {@code severity} bounded to the
 * {@code OnmsSeverity} enum (7 values). No per-node/per-ip labels — those
 * would explode cardinality.
 */
public final class AlarmdDomainMetrics {

    private AlarmdDomainMetrics() {}

    public static final String ALARMS_CREATED          = "deltav.alarmd.alarms.created";
    public static final String ALARMS_REDUCED          = "deltav.alarmd.alarms.reduced";
    public static final String ALARMS_ACKNOWLEDGED     = "deltav.alarmd.alarms.acknowledged";
    public static final String ALARMS_UNACKNOWLEDGED   = "deltav.alarmd.alarms.unacknowledged";
    public static final String ALARMS_SEVERITY_UPDATED = "deltav.alarmd.alarms.severity.updated";
    public static final String ALARMS_ARCHIVED         = "deltav.alarmd.alarms.archived";
    public static final String ALARMS_DELETED          = "deltav.alarmd.alarms.deleted";

    static final String TAG_SEVERITY = "severity";
}
