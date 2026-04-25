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
 * Names become {@code deltav_perspective_*} at {@code /actuator/prometheus}
 * after Micrometer's dot-to-underscore flattening.
 *
 * <p>Architecture note (vs pollerd): horizon's {@code PerspectivePollerd}
 * uses a Quartz-based scheduler and a private {@code lambda$execute$0}
 * inside {@code PerspectivePollJob} as its per-poll callback. There is no
 * public {@code PollContext}-equivalent seam to subclass without modifying
 * horizon source. As a result, beta2 instrumentation is bounded to the
 * {@code EventForwarder} wrap (outage UEI counting) — the same pattern
 * applied to discovery's daemon. Per-poll response-time {@code deltav_*}
 * counters and Phase 3 Kafka publishing for perspective polls require a
 * deeper hook (e.g., a wrapping {@code PersisterFactory}) and are tracked
 * as a post-beta2 follow-up.
 */
public final class PerspectivePollerdDomainMetrics {

    private PerspectivePollerdDomainMetrics() {}

    public static final String EVENTS_FORWARDED          = "deltav.perspective.events.forwarded";
    public static final String SERVICE_LOST_TOTAL        = "deltav.perspective.service.lost";
    public static final String SERVICE_REGAINED_TOTAL    = "deltav.perspective.service.regained";
}
