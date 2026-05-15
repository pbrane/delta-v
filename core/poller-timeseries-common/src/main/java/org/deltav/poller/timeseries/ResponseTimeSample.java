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
package org.deltav.poller.timeseries;

import org.deltav.timeseries.proto.ProducerType;

/**
 * Daemon-neutral value object describing one completed service poll's
 * response-time measurement. Pollerd and PerspectivePollerd each adapt their
 * horizon-specific poll objects into this record, which the shared
 * {@link ResponseTimePublisher} consumes.
 *
 * @param nodeId         monitored node id
 * @param serviceName    monitored service name (e.g. "ICMP", "HTTP-8080")
 * @param location       Minion location (Pollerd) or perspective location
 *                       (PerspectivePollerd) that ran the poll
 * @param responseTimeMs measured response time, milliseconds
 * @param timestampMs    poll completion timestamp, epoch millis
 * @param producerType   protobuf producer discriminator
 * @param producerLabel  Micrometer {@code producer} tag value
 */
public record ResponseTimeSample(
        int nodeId,
        String serviceName,
        String location,
        double responseTimeMs,
        long timestampMs,
        ProducerType producerType,
        String producerLabel) {
}
