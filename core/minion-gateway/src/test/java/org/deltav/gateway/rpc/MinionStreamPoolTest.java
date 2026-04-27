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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MinionStreamPoolTest {

    @Test
    void emptyPool_returnsNullForUnknownLocation() {
        MinionStreamPool pool = new MinionStreamPool();
        assertThat(pool.pickStream("Default")).isNull();
    }

    @Test
    void registeredStream_returnsStreamForItsLocation() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> observer = mock(StreamObserver.class);
        pool.register("Default", "minion-A", observer);

        assertThat(pool.pickStream("Default")).isSameAs(observer);
    }

    @Test
    void multipleStreamsInSameLocation_roundRobin() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> a = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> b = mock(StreamObserver.class);
        pool.register("Default", "minion-A", a);
        pool.register("Default", "minion-B", b);

        assertThat(pool.pickStream("Default")).isSameAs(a);
        assertThat(pool.pickStream("Default")).isSameAs(b);
        assertThat(pool.pickStream("Default")).isSameAs(a);
    }

    @Test
    void unregisteredStream_noLongerSelectable() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> a = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> b = mock(StreamObserver.class);
        pool.register("Default", "minion-A", a);
        pool.register("Default", "minion-B", b);
        pool.unregister("Default", "minion-A");

        assertThat(pool.pickStream("Default")).isSameAs(b);
        assertThat(pool.pickStream("Default")).isSameAs(b);
    }

    @Test
    void emptyAfterAllUnregister_returnsNull() {
        MinionStreamPool pool = new MinionStreamPool();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> a = mock(StreamObserver.class);
        pool.register("Default", "minion-A", a);
        pool.unregister("Default", "minion-A");

        assertThat(pool.pickStream("Default")).isNull();
    }
}
