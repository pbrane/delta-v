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
package org.deltav.netmgt.poller.boot;

/**
 * Canonical Micrometer meter names for pollerd domain signals. Names become
 * {@code deltav_pollerd_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>The instrumentation point is {@link org.opennms.netmgt.poller.DefaultPollContext}'s
 * three callback methods:
 * <ul>
 *   <li>{@code trackPoll} — fires once per poll completion. Tagged by
 *       {@code location} (bounded to configured Minion locations) and
 *       {@code result} (bounded to {@code PollStatus} status names).</li>
 *   <li>{@code openOutage} / {@code resolveOutage} — fire on outage state
 *       transitions. Tagged by {@code location} only; service name is too
 *       cardinal to label.</li>
 * </ul>
 *
 * <p>Response-time distributions feed both Micrometer (for direct alerting on
 * percentile latency) and the Phase 3 Kafka time-series topic (for storage
 * in VictoriaMetrics).
 */
public final class PollerdDomainMetrics {

    private PollerdDomainMetrics() {}

    public static final String POLLS_COMPLETED       = "deltav.pollerd.polls.completed";
    public static final String POLL_DURATION         = "deltav.pollerd.poll.duration";
    public static final String OUTAGES_OPENED        = "deltav.pollerd.outages.opened";
    public static final String OUTAGES_RESOLVED      = "deltav.pollerd.outages.resolved";

    static final String TAG_LOCATION = "location";
    static final String TAG_RESULT   = "result";
}
