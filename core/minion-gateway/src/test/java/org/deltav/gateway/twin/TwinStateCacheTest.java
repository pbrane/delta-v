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
package org.deltav.gateway.twin;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TwinStateCacheTest {

    @Test
    void emptyCache_returnsNullForUnknownKey() {
        TwinStateCache cache = new TwinStateCache();
        assertThat(cache.get("k", "L")).isNull();
    }

    @Test
    void putThenGet_returnsStoredEntry() {
        TwinStateCache cache = new TwinStateCache();
        ByteString state = ByteString.copyFromUtf8("{\"x\":1}");
        cache.put("k", "L", state, 5, "session-1");

        TwinStateCache.Entry e = cache.get("k", "L");
        assertThat(e).isNotNull();
        assertThat(e.state()).isEqualTo(state);
        assertThat(e.version()).isEqualTo(5);
        assertThat(e.sessionId()).isEqualTo("session-1");
    }

    @Test
    void put_overwritesPriorEntry() {
        TwinStateCache cache = new TwinStateCache();
        cache.put("k", "L", ByteString.copyFromUtf8("v1"), 1, "session-1");
        cache.put("k", "L", ByteString.copyFromUtf8("v2"), 2, "session-1");

        TwinStateCache.Entry e = cache.get("k", "L");
        assertThat(e.state()).isEqualTo(ByteString.copyFromUtf8("v2"));
        assertThat(e.version()).isEqualTo(2);
    }

    @Test
    void differentLocations_areIndependent() {
        TwinStateCache cache = new TwinStateCache();
        cache.put("k", "L1", ByteString.copyFromUtf8("v1"), 1, "session-A");
        cache.put("k", "L2", ByteString.copyFromUtf8("v2"), 1, "session-B");

        assertThat(cache.get("k", "L1").state()).isEqualTo(ByteString.copyFromUtf8("v1"));
        assertThat(cache.get("k", "L2").state()).isEqualTo(ByteString.copyFromUtf8("v2"));
    }
}
