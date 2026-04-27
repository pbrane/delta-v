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
package org.deltav.gateway.rpc;

import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.RpcRequest;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-location pool of currently-open Minion bidi streams. The competing-
 * consumer property of the Kafka RPC pattern is preserved here: multiple
 * Minions in a location each register their stream; the gateway picks one
 * per RPC via round-robin so work spreads across the pool.
 *
 * <p>Per Decision 1 of the v1.2.0-rc2 decisions doc, this is the gRPC
 * equivalent of Kafka's consumer group: registration on stream open,
 * unregister on stream close, redispatch is the responsibility of the
 * caller (see {@code RpcChannelDispatcher}).
 */
@Component
public class MinionStreamPool {

    private final Map<String, LocationPool> pools = new ConcurrentHashMap<>();

    public void register(String location, String minionId, StreamObserver<RpcRequest> stream) {
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(minionId, "minionId");
        Objects.requireNonNull(stream, "stream");
        pools.computeIfAbsent(location, k -> new LocationPool()).add(minionId, stream);
    }

    public void unregister(String location, String minionId) {
        LocationPool pool = pools.get(location);
        if (pool != null) {
            pool.remove(minionId);
        }
    }

    public StreamObserver<RpcRequest> pickStream(String location) {
        LocationPool pool = pools.get(location);
        return pool == null ? null : pool.pickRoundRobin();
    }

    public List<StreamObserver<RpcRequest>> siblingStreams(String location, StreamObserver<RpcRequest> exclude) {
        LocationPool pool = pools.get(location);
        return pool == null ? List.of() : pool.siblingsOf(exclude);
    }

    private static final class LocationPool {
        private volatile List<Entry> entries = new ArrayList<>();
        private final AtomicInteger cursor = new AtomicInteger();

        synchronized void add(String minionId, StreamObserver<RpcRequest> stream) {
            List<Entry> next = new ArrayList<>(entries);
            next.removeIf(e -> e.minionId.equals(minionId));
            next.add(new Entry(minionId, stream));
            entries = next;
        }

        synchronized void remove(String minionId) {
            List<Entry> next = new ArrayList<>(entries);
            next.removeIf(e -> e.minionId.equals(minionId));
            entries = next;
        }

        StreamObserver<RpcRequest> pickRoundRobin() {
            List<Entry> snapshot = entries;
            if (snapshot.isEmpty()) {
                return null;
            }
            int idx = Math.floorMod(cursor.getAndIncrement(), snapshot.size());
            return snapshot.get(idx).stream;
        }

        List<StreamObserver<RpcRequest>> siblingsOf(StreamObserver<RpcRequest> exclude) {
            List<Entry> snapshot = entries;
            List<StreamObserver<RpcRequest>> result = new ArrayList<>(snapshot.size());
            for (Entry e : snapshot) {
                if (e.stream != exclude) {
                    result.add(e.stream);
                }
            }
            return result;
        }

        private record Entry(String minionId, StreamObserver<RpcRequest> stream) {}
    }
}
