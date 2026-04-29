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
package org.deltav.minion.common.grpc;

import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Per-(sink-module-id) cache of currently-open Minion-side
 * {@link StreamObserver} instances. Each unique module id (e.g. "Syslog",
 * "Trap", "Telemetry-IPFIX") gets its own entry; the supplier opens a new
 * gRPC client-streaming stream on cache miss.
 *
 * <p>{@link #reset(String)} drops the cached entry without closing it;
 * the caller is responsible for invoking {@code onCompleted()} or
 * {@code onError()} on the returned observer, after which the entry can
 * be reset so the next send opens a fresh stream. This mirrors rc1's
 * {@code GrpcMessageDispatcherFactory.heartbeatStream} reset-on-error
 * pattern.
 *
 * <p>Stream observers are not type-parameterised at the registry level
 * because protobuf message types differ per sink; the caller's supplier
 * binds the concrete type. The registry stores them as raw
 * {@code StreamObserver<?>} and the caller casts on get-or-open.
 */
public class SinkStreamRegistry {

    @SuppressWarnings("rawtypes")
    private final Map<String, StreamObserver> entries = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> StreamObserver<T> getOrOpen(String moduleId, Supplier<StreamObserver<T>> opener) {
        Objects.requireNonNull(moduleId, "moduleId");
        Objects.requireNonNull(opener, "opener");
        return entries.computeIfAbsent(moduleId, k -> opener.get());
    }

    /**
     * Drop the cached entry for {@code moduleId}. Next {@link #getOrOpen}
     * for that id opens a fresh stream.
     *
     * <p>Known race: a producer thread that observed a stream error and
     * another caller that already opened a fresh stream can collide — the
     * producer's reset will evict the healthy fresh stream, costing one
     * message until the next reopen. Window is microseconds and matches
     * rc1's {@code GrpcMessageDispatcherFactory} pattern. A
     * {@code Map.remove(key, expectedValue)} variant could close it but
     * adds API surface; deferred until evidence of operational impact.
     */
    public void reset(String moduleId) {
        entries.remove(moduleId);
    }
}
