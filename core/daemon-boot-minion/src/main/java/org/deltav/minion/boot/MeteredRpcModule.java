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
package org.deltav.minion.boot;

import java.util.concurrent.CompletableFuture;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.opennms.core.rpc.api.RpcModule;
import org.opennms.core.rpc.api.RpcRequest;
import org.opennms.core.rpc.api.RpcResponse;

/**
 * Decorator around any {@link RpcModule} that increments Micrometer counters
 * for each request executed on the Minion side, and times the asynchronous
 * completion of the returned {@link CompletableFuture}.
 *
 * <p>Pre-resolves the per-module counter and timer instances at construction
 * time so the hot path is a single map lookup per call. Module id is bounded
 * to the compiled-in RPC module set (currently 7 modules), so cardinality is
 * safe.
 *
 * <p>Marshal/unmarshal/createResponseWithException all delegate without
 * instrumentation — those are pure marshaling operations and not part of the
 * request-rate signal we want.
 */
public class MeteredRpcModule<S extends RpcRequest, T extends RpcResponse>
        implements RpcModule<S, T> {

    private final RpcModule<S, T> delegate;
    private final MeterRegistry registry;
    private final Counter received;
    private final Counter failed;
    private final Timer duration;

    public MeteredRpcModule(RpcModule<S, T> delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
        String moduleId = delegate.getId();
        this.received = registry.counter(MinionDomainMetrics.RPC_RECEIVED,
                MinionDomainMetrics.TAG_MODULE, moduleId);
        this.failed = registry.counter(MinionDomainMetrics.RPC_FAILED,
                MinionDomainMetrics.TAG_MODULE, moduleId);
        this.duration = Timer.builder(MinionDomainMetrics.RPC_DURATION)
                .tag(MinionDomainMetrics.TAG_MODULE, moduleId)
                .register(registry);
    }

    @Override
    public CompletableFuture<T> execute(S request) {
        received.increment();
        Timer.Sample sample = Timer.start(registry);
        CompletableFuture<T> future = delegate.execute(request);
        future.whenComplete((response, throwable) -> {
            sample.stop(duration);
            if (throwable != null) {
                failed.increment();
            }
        });
        return future;
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public String marshalRequest(S request) {
        return delegate.marshalRequest(request);
    }

    @Override
    public S unmarshalRequest(String s) {
        return delegate.unmarshalRequest(s);
    }

    @Override
    public String marshalResponse(T response) {
        return delegate.marshalResponse(response);
    }

    @Override
    public T unmarshalResponse(String s) {
        return delegate.unmarshalResponse(s);
    }

    @Override
    public T createResponseWithException(Throwable throwable) {
        return delegate.createResponseWithException(throwable);
    }
}
