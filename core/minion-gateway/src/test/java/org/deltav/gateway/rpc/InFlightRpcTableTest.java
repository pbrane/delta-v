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

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class InFlightRpcTableTest {

    @Test
    void recordedRequest_isRetrievableByRpcId() {
        InFlightRpcTable table = new InFlightRpcTable();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> stream = mock(StreamObserver.class);
        RpcRequest req = RpcRequest.newBuilder().setRpcId("abc").build();

        table.record("abc", stream, req, Instant.now().plusSeconds(30));

        assertThat(table.findEntry("abc")).isNotNull();
        assertThat(table.findEntry("abc").request()).isEqualTo(req);
    }

    @Test
    void completed_evictsEntry() {
        InFlightRpcTable table = new InFlightRpcTable();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> stream = mock(StreamObserver.class);
        table.record("abc", stream, RpcRequest.newBuilder().setRpcId("abc").build(), Instant.now().plusSeconds(30));

        table.complete("abc");

        assertThat(table.findEntry("abc")).isNull();
    }

    @Test
    void evictByStream_returnsAndRemovesAllForThatStream() {
        InFlightRpcTable table = new InFlightRpcTable();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> streamA = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> streamB = mock(StreamObserver.class);
        Instant deadline = Instant.now().plusSeconds(30);
        table.record("a1", streamA, RpcRequest.newBuilder().setRpcId("a1").build(), deadline);
        table.record("a2", streamA, RpcRequest.newBuilder().setRpcId("a2").build(), deadline);
        table.record("b1", streamB, RpcRequest.newBuilder().setRpcId("b1").build(), deadline);

        List<InFlightRpcTable.Entry> evicted = table.evictByStream(streamA);

        assertThat(evicted).hasSize(2);
        assertThat(evicted).extracting(InFlightRpcTable.Entry::rpcId).containsExactlyInAnyOrder("a1", "a2");
        assertThat(table.findEntry("a1")).isNull();
        assertThat(table.findEntry("a2")).isNull();
        assertThat(table.findEntry("b1")).isNotNull();
    }

    @Test
    void evictExpired_returnsEntriesPastDeadline() {
        InFlightRpcTable table = new InFlightRpcTable();
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> stream = mock(StreamObserver.class);
        Instant past = Instant.now().minus(Duration.ofSeconds(5));
        Instant future = Instant.now().plus(Duration.ofSeconds(60));
        table.record("expired", stream, RpcRequest.newBuilder().setRpcId("expired").build(), past);
        table.record("alive", stream, RpcRequest.newBuilder().setRpcId("alive").build(), future);

        List<InFlightRpcTable.Entry> evicted = table.evictExpired(Instant.now());

        assertThat(evicted).extracting(InFlightRpcTable.Entry::rpcId).containsExactly("expired");
        assertThat(table.findEntry("alive")).isNotNull();
    }
}
