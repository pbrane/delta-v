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
package org.deltav.netmgt.telemetry.boot;

/**
 * Canonical Micrometer meter names for telemetryd domain signals. Names become
 * {@code deltav_telemetryd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>Tagged by {@code module} (the SinkModule id, e.g. {@code Telemetry-Netflow-5},
 * {@code Telemetry-IPFIX}, {@code Telemetry-sFlow}). The set is bounded to the
 * configured telemetry queues — typically 4–8 protocols.
 */
public final class TelemetrydDomainMetrics {

    private TelemetrydDomainMetrics() {}

    public static final String PACKETS_RECEIVED   = "deltav.telemetryd.packets.received";
    public static final String PACKETS_DISPATCHED = "deltav.telemetryd.packets.dispatched";
    public static final String DISPATCH_FAILURES  = "deltav.telemetryd.dispatch.failures";
    public static final String DISPATCH_DURATION  = "deltav.telemetryd.dispatch.duration";

    static final String TAG_MODULE = "module";
}
