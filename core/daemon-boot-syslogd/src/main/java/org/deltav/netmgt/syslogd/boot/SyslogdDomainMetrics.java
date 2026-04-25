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
package org.deltav.netmgt.syslogd.boot;

/**
 * Canonical Micrometer meter names for syslogd domain signals. Names become
 * {@code deltav_syslogd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>These are the native delta-v counters; horizon's existing Dropwizard
 * timers (consumer/toEvent/broadcast) are surfaced separately as
 * {@code opennms_*} via {@link org.deltav.horizon.metrics.HorizonMetricsBridge}.
 *
 * <p>Tagged by {@code location} (Minion location), bounded to the configured
 * locations. No per-source-address labels.
 */
public final class SyslogdDomainMetrics {

    private SyslogdDomainMetrics() {}

    public static final String MESSAGE_LOGS_RECEIVED = "deltav.syslogd.messagelogs.received";
    public static final String MESSAGES_IN_BATCH     = "deltav.syslogd.messages.in.batch";
    public static final String HANDLE_DURATION       = "deltav.syslogd.handle.duration";

    static final String TAG_LOCATION = "location";
}
