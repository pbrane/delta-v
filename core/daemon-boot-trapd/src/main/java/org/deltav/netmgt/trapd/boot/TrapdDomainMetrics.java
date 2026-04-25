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
package org.deltav.netmgt.trapd.boot;

/**
 * Canonical Micrometer meter names for trapd domain signals. Names become
 * {@code deltav_trapd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>{@code TRAPLOGS_RECEIVED} counts each {@code TrapLogDTO} batch arriving
 * from a Minion via the Sink topic. {@code TRAPS_IN_BATCH} is incremented by
 * the size of each batch — operators want both signals because batch shape
 * (small frequent batches vs. large rare batches) is itself diagnostic.
 *
 * <p>Tagged by {@code location} (Minion location), bounded to the configured
 * locations — typically a small finite set. No per-trap-source-address
 * labels (would explode cardinality).
 */
public final class TrapdDomainMetrics {

    private TrapdDomainMetrics() {}

    public static final String TRAPLOGS_RECEIVED  = "deltav.trapd.traplogs.received";
    public static final String TRAPS_IN_BATCH     = "deltav.trapd.traps.in.batch";
    public static final String HANDLE_DURATION    = "deltav.trapd.handle.duration";

    static final String TAG_LOCATION = "location";
}
