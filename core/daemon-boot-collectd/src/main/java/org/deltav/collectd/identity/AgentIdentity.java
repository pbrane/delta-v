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
package org.deltav.collectd.identity;

/**
 * Immutable agent identity captured from a {@code CollectionAgent} at RPC
 * dispatch time and consumed by {@code TimeseriesKafkaPersister} at publish
 * time.
 *
 * <p>Validation of {@code nodeId} is intentionally deferred to the persist
 * site (persist-time-only fail-fast): the record accepts any int so capture
 * never aborts a collection cycle. A {@code nodeId <= 0} surfaces as a
 * publisher-side {@code IllegalStateException} caught by
 * {@code FanoutPersister}, which increments the kafka failures counter.</p>
 *
 * <p>{@code location} is normalized to the empty string when null — Default
 * location setups sometimes return {@code null}. Downstream code may assume
 * non-null.</p>
 */
public record AgentIdentity(int nodeId, String location) {
    public AgentIdentity {
        if (location == null) {
            location = "";
        }
    }
}
