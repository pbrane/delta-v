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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks in-flight RPC requests dispatched by the gateway. Each entry
 * remembers the stream it was sent to (for redispatch on stream close),
 * the deadline (for expiry sweep), and the original request body (so
 * redispatch can resend the same payload to a sibling stream).
 *
 * <p>Per Decision 1 sub-decision 1-iii of the v1.2.0-rc2 decisions doc:
 * eviction is triggered both on response delivery (success path) and on
 * stream close (redispatch path). Stream close evicts ALL entries for
 * that stream regardless of whether redispatch succeeds, to prevent leaks
 * under stream flap.
 */
@Component
public class InFlightRpcTable {

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    public void record(String rpcId, StreamObserver<RpcRequest> stream,
                       RpcRequest request, Instant deadline) {
        entries.put(rpcId, new Entry(rpcId, stream, request, deadline));
    }

    public Entry findEntry(String rpcId) {
        return entries.get(rpcId);
    }

    public void complete(String rpcId) {
        entries.remove(rpcId);
    }

    public List<Entry> evictByStream(StreamObserver<RpcRequest> closedStream) {
        List<Entry> evicted = new ArrayList<>();
        entries.entrySet().removeIf(e -> {
            if (e.getValue().stream() == closedStream) {
                evicted.add(e.getValue());
                return true;
            }
            return false;
        });
        return Collections.unmodifiableList(evicted);
    }

    public List<Entry> evictExpired(Instant now) {
        List<Entry> evicted = new ArrayList<>();
        entries.entrySet().removeIf(e -> {
            if (e.getValue().deadline().isBefore(now)) {
                evicted.add(e.getValue());
                return true;
            }
            return false;
        });
        return Collections.unmodifiableList(evicted);
    }

    public int size() {
        return entries.size();
    }

    public record Entry(String rpcId,
                        StreamObserver<RpcRequest> stream,
                        RpcRequest request,
                        Instant deadline) {}
}
