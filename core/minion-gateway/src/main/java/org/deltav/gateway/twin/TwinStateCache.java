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
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-(consumer_key, location) Twin state cache. The gateway reads-through
 * from this cache when a Minion opens a Twin subscription — the first
 * TwinUpdate sent on a freshly-opened stream is always a full snapshot
 * (Decision 2 sub-decision 2-i).
 *
 * <p>Populated by {@link TwinChannelDispatcher} consuming horizon's
 * {@code DeltaV.twin.response.<location>} Kafka topic on gateway startup
 * (read-to-tail) and on every subsequent state change. Per Decision 2
 * sub-decision 2-iii: state is recovered from Kafka, not from
 * inter-gateway coordination, so multi-instance deployments work without
 * coordination.
 */
@Component
public class TwinStateCache {

    private final Map<Key, Entry> entries = new ConcurrentHashMap<>();

    public Entry get(String consumerKey, String location) {
        Objects.requireNonNull(consumerKey, "consumerKey");
        Objects.requireNonNull(location, "location");
        return entries.get(new Key(consumerKey, location));
    }

    public void put(String consumerKey, String location, ByteString state, int version, String sessionId) {
        Objects.requireNonNull(consumerKey, "consumerKey");
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(sessionId, "sessionId");
        entries.put(new Key(consumerKey, location), new Entry(state, version, sessionId));
    }

    public int size() {
        return entries.size();
    }

    public record Key(String consumerKey, String location) {}
    public record Entry(ByteString state, int version, String sessionId) {}
}
