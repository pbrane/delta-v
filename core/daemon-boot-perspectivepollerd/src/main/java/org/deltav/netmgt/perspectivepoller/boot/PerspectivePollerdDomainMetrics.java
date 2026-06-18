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
package org.deltav.netmgt.perspectivepoller.boot;

/**
 * Canonical Micrometer meter names for perspectivepollerd domain signals.
 * Per-poll instrumentation names become {@code deltav_perspective_*} at
 * {@code /actuator/prometheus} after Micrometer's dot-to-underscore flattening;
 * the FR9 config-gap gauge uses the full per-daemon prefix
 * ({@code deltav_perspectivepollerd_services_unscheduled}) for symmetry with
 * pollerd's {@code deltav_pollerd_services_unscheduled}.
 *
 * <p>Instrumentation seam: {@link InstrumentedPerspectivePollerd} overrides
 * horizon's {@code persistResponseTimeData(PerspectivePolledService, PollStatus)}
 * (called once per RPC-successful poll completion from
 * {@code PerspectivePollJob.execute()}). RPC-level failures (timeouts,
 * disconnects) never reach this seam — see {@code feedback_rpc_timeout_no_outages}.
 *
 * <p>The {@code EventForwarder} wrap ({@link CountingPerspectiveEventForwarder})
 * supplies the outage UEI counters; that path remains unchanged.
 */
public final class PerspectivePollerdDomainMetrics {

    private PerspectivePollerdDomainMetrics() {}

    public static final String EVENTS_FORWARDED          = "deltav.perspective.events.forwarded";
    public static final String SERVICE_LOST_TOTAL        = "deltav.perspective.service.lost";
    public static final String SERVICE_REGAINED_TOTAL    = "deltav.perspective.service.regained";

    public static final String POLLS_COMPLETED           = "deltav.perspective.polls.completed";
    public static final String POLL_DURATION             = "deltav.perspective.poll.duration";

    /**
     * Labeled gauge (one series per unschedulable perspective service type) for catalog config
     * gaps (FR9). Full per-daemon prefix, mirroring pollerd's {@code deltav_pollerd_services_unscheduled}.
     */
    public static final String SERVICES_UNSCHEDULED      = "deltav.perspectivepollerd.services.unscheduled";

    static final String TAG_LOCATION    = "location";
    static final String TAG_PERSPECTIVE = "perspective";
    static final String TAG_RESULT      = "result";
    static final String TAG_SERVICE     = "service";
}
