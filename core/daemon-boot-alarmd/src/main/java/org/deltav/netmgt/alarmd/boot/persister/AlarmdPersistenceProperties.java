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
package org.deltav.netmgt.alarmd.boot.persister;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code deltav.alarmd.persistence.*} configuration for
 * {@link DeltavAlarmPersister}.
 *
 * <p>Two modes are recognized:
 * <ul>
 *   <li>{@code kafka-only} (default at v1.3.0 GA): reduction lookups hit the
 *       in-memory {@code ReductionCache}; the resulting {@link
 *       org.opennms.netmgt.model.OnmsAlarm} is fanned out via
 *       {@link org.opennms.netmgt.dao.api.AlarmEntityNotifier} and never
 *       written to PostgreSQL.</li>
 *   <li>{@code dual-write} (one-release escape hatch): the wrapped horizon
 *       {@code AlarmPersisterImpl} is invoked, preserving the legacy
 *       PG-write + AlarmEntityNotifier fan-out chain.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "deltav.alarmd.persistence")
public class AlarmdPersistenceProperties {

    /**
     * Persistence mode. Default is {@code dual-write} until the v1.3.0
     * failure-mode tabletop passes; {@code kafka-only} is the GA target and is
     * opt-in via {@code DELTAV_ALARMD_PERSISTENCE_MODE} until that gate clears.
     */
    private String mode = "dual-write";

    public String getMode() {
        return mode;
    }

    public void setMode(String v) {
        this.mode = v;
    }

    public boolean isKafkaOnly() {
        return "kafka-only".equalsIgnoreCase(mode);
    }

    public boolean isDualWrite() {
        return "dual-write".equalsIgnoreCase(mode);
    }
}
