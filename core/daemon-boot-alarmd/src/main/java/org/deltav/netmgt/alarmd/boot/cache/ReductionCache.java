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
package org.deltav.netmgt.alarmd.boot.cache;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.deltav.alarms.proto.AlarmState;
import org.springframework.stereotype.Component;

/**
 * In-memory map of {@code reduction_key → latest AlarmState}. Bootstrapped at
 * startup by {@link ReductionCacheKafkaBootstrap} replaying the compacted
 * {@code deltav-alarms-state-change} topic from offset 0; live-tails alarmd's
 * own publishes thereafter.
 *
 * <p>Used by {@code DeltavAlarmPersister} for event reduction lookups instead
 * of querying {@code AlarmDao} (spec §3.1).
 */
@Component
public class ReductionCache {

    private final ConcurrentHashMap<String, AlarmState> map = new ConcurrentHashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public Optional<AlarmState> get(String reductionKey) {
        return Optional.ofNullable(map.get(reductionKey));
    }

    public void applyUpdate(String reductionKey, AlarmState alarm) {
        map.put(reductionKey, alarm);
    }

    public void applyTombstone(String reductionKey) {
        map.remove(reductionKey);
    }

    public int size() {
        return map.size();
    }

    public void markReady() {
        ready.set(true);
    }

    public boolean isReady() {
        return ready.get();
    }
}
