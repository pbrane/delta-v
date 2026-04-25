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
package org.deltav.netmgt.enlinkd.boot;

/**
 * Canonical Micrometer meter names for enlinkd domain signals. Names become
 * {@code deltav_enlinkd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>The instrumentation point is the central {@code OnmsTopologyDao.update}
 * call — every protocol's updater funnels its topology messages through this
 * single method, so wrapping it captures activity from all 9 protocol updaters
 * (LLDP, CDP, OSPF, OSPFAREA, ISIS, BRIDGE, NETWORKROUTER, NODES, USERDEFINED)
 * without subclassing each one.
 *
 * <p>Tagged by {@code protocol} (bounded enum) and {@code status} (UPDATE or
 * DELETE — bounded). Updater-registration counters give a one-shot signal at
 * daemon start to confirm the bean graph wired up correctly.
 */
public final class EnlinkdDomainMetrics {

    private EnlinkdDomainMetrics() {}

    public static final String TOPOLOGY_UPDATES        = "deltav.enlinkd.topology.updates";
    public static final String TOPOLOGY_UPDATERS_ACTIVE = "deltav.enlinkd.topology.updaters.active";

    static final String TAG_PROTOCOL = "protocol";
    static final String TAG_STATUS   = "status";
}
