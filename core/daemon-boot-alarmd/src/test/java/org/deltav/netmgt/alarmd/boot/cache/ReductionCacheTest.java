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

import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReductionCacheTest {

    private AlarmState alarmFor(String rk, int count) {
        return AlarmState.newBuilder()
                .setReductionKey(rk).setUei("uei/x")
                .setSeverity(AlarmState.Severity.MAJOR)
                .setCount(count).build();
    }

    @Test
    void putThenGetReturnsLatest() {
        ReductionCache cache = new ReductionCache();
        cache.applyUpdate("rk", alarmFor("rk", 1));
        cache.applyUpdate("rk", alarmFor("rk", 2));

        Optional<AlarmState> latest = cache.get("rk");

        assertThat(latest).isPresent();
        assertThat(latest.get().getCount()).isEqualTo(2);
    }

    @Test
    void tombstoneEvicts() {
        ReductionCache cache = new ReductionCache();
        cache.applyUpdate("rk", alarmFor("rk", 5));
        assertThat(cache.size()).isEqualTo(1);

        cache.applyTombstone("rk");

        assertThat(cache.get("rk")).isEmpty();
        assertThat(cache.size()).isZero();
    }

    @Test
    void readyFlagFlipsAfterMarkReady() {
        ReductionCache cache = new ReductionCache();
        assertThat(cache.isReady()).isFalse();
        cache.markReady();
        assertThat(cache.isReady()).isTrue();
    }

    @Test
    void getOnMissingKeyReturnsEmpty() {
        assertThat(new ReductionCache().get("nope")).isEmpty();
    }
}
