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
package org.deltav.minion.boot;

/**
 * Canonical Micrometer meter names for the Minion's native delta-v signals.
 * Names become {@code deltav_minion_*} at {@code /actuator/prometheus} after
 * Micrometer's dot-to-underscore flattening.
 *
 * <p>Per-module counters are tagged by RPC module id ({@code Echo},
 * {@code Detect}, {@code Collect}, {@code Poll}, {@code Ping},
 * {@code PingSweep}, {@code SnmpProxy}, etc.). The set is bounded to the
 * compiled-in module set (currently 7 modules). Per-call duration is a
 * timer keyed by the same module tag.
 *
 * <p>Note (vs the existing {@code opennms_*} meters via PR #216's
 * horizon-metric-bridge): horizon's {@code KafkaRpcServerManager} already
 * surfaces its internal Codahale {@code MetricRegistry} as
 * {@code opennms_*} metrics. The {@code deltav_minion_*} meters added here
 * are a separate, native Micrometer source that an HPA can target without
 * inheriting any decisions baked into horizon's bridged names.
 */
public final class MinionDomainMetrics {

    private MinionDomainMetrics() {}

    public static final String RPC_RECEIVED  = "deltav.minion.rpc.received";
    public static final String RPC_FAILED    = "deltav.minion.rpc.failed";
    public static final String RPC_DURATION  = "deltav.minion.rpc.duration";

    static final String TAG_MODULE = "module";
}
