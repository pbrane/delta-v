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

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RpcChannelDispatcherTest {

    @Test
    void onStreamClosed_redispatchesToSurvivingSibling() {
        MinionStreamPool pool = mock(MinionStreamPool.class);
        InFlightRpcTable table = new InFlightRpcTable();
        RpcResponsePublisher publisher = mock(RpcResponsePublisher.class);
        RpcChannelDispatcher dispatcher = new RpcChannelDispatcher(pool, table, publisher);

        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> closedStream = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> sibling = mock(StreamObserver.class);
        when(pool.siblingStreams("Default", closedStream)).thenReturn(java.util.List.of(sibling));

        RpcRequest req = RpcRequest.newBuilder().setRpcId("xyz").setLocation("Default").build();
        table.record("xyz", closedStream, req, Instant.now().plusSeconds(30));

        dispatcher.onStreamClosed("Default", closedStream);

        verify(sibling).onNext(req);
        assertThat(table.findEntry("xyz")).isNotNull();
        assertThat(table.findEntry("xyz").stream()).isSameAs(sibling);
    }

    @Test
    void onStreamClosed_noSibling_dropsRequestWithoutFailing() {
        MinionStreamPool pool = mock(MinionStreamPool.class);
        InFlightRpcTable table = new InFlightRpcTable();
        RpcResponsePublisher publisher = mock(RpcResponsePublisher.class);
        RpcChannelDispatcher dispatcher = new RpcChannelDispatcher(pool, table, publisher);

        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> closedStream = mock(StreamObserver.class);
        when(pool.siblingStreams("Default", closedStream)).thenReturn(java.util.List.of());

        RpcRequest req = RpcRequest.newBuilder().setRpcId("xyz").setLocation("Default").build();
        table.record("xyz", closedStream, req, Instant.now().plusSeconds(30));

        dispatcher.onStreamClosed("Default", closedStream);

        // Per Decision 1 sub-decision 1-i: empty pool -> caller's existing
        // dispatcher deadline absorbs. Gateway does not synthesize a failure.
        assertThat(table.findEntry("xyz")).isNull();
    }

    @Test
    void onResponse_forwardsToPublisher_andEvictsTableEntry() {
        MinionStreamPool pool = mock(MinionStreamPool.class);
        InFlightRpcTable table = new InFlightRpcTable();
        RpcResponsePublisher publisher = mock(RpcResponsePublisher.class);
        RpcChannelDispatcher dispatcher = new RpcChannelDispatcher(pool, table, publisher);

        @SuppressWarnings("unchecked")
        StreamObserver<RpcRequest> stream = mock(StreamObserver.class);
        RpcRequest req = RpcRequest.newBuilder().setRpcId("xyz").build();
        table.record("xyz", stream, req, Instant.now().plusSeconds(30));

        org.deltav.minion.grpc.v1.RpcResponse rsp =
            org.deltav.minion.grpc.v1.RpcResponse.newBuilder().setRpcId("xyz").build();
        dispatcher.handle(rsp);

        verify(publisher).publish(rsp);
        assertThat(table.findEntry("xyz")).isNull();
    }

    @Test
    void dispatch_noStream_publishesFastErrorResponse() {
        MinionStreamPool pool = mock(MinionStreamPool.class);
        InFlightRpcTable table = new InFlightRpcTable();
        RpcResponsePublisher publisher = mock(RpcResponsePublisher.class);
        RpcChannelDispatcher dispatcher = new RpcChannelDispatcher(pool, table, publisher);

        when(pool.pickStream("nl6-lab")).thenReturn(null);

        RpcRequest req = RpcRequest.newBuilder()
            .setRpcId("xyz").setLocation("nl6-lab").build();

        // No Minion at the location: rather than dropping (the caller then waits its
        // full ~30s RPC deadline, and many services targeting a minion-less location
        // flood the RPC path and starve other locations), publish a fast error
        // response so the caller fails immediately and marks the service UNKNOWN
        // (an RPC error, not a false outage — feedback_rpc_timeout_no_outages). See #368.
        dispatcher.dispatch(req);

        assertThat(table.findEntry("xyz")).isNull(); // nothing to track without a stream
        org.mockito.ArgumentCaptor<org.deltav.minion.grpc.v1.RpcResponse> cap =
            org.mockito.ArgumentCaptor.forClass(org.deltav.minion.grpc.v1.RpcResponse.class);
        verify(publisher).publish(cap.capture());
        assertThat(cap.getValue().getRpcId()).isEqualTo("xyz");
        assertThat(cap.getValue().getErrorMessage()).contains("nl6-lab");
    }
}
