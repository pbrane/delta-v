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
import org.deltav.minion.grpc.v1.RpcRequest;
import org.deltav.minion.grpc.v1.RpcResponse;
import org.junit.jupiter.api.Test;
import org.opennms.core.rpc.api.RpcModule;
import org.opennms.distributed.core.api.MinionIdentity;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class MinionRpcStreamClientTest {

    @Test
    void onIncomingRequest_dispatchesToRpcModuleAndSendsResponse() {
        @SuppressWarnings("rawtypes")
        RpcModule module = mock(RpcModule.class);
        when(module.getId()).thenReturn("Echo");
        when(module.unmarshalRequest(anyString())).thenReturn(mock(org.opennms.core.rpc.api.RpcRequest.class));

        org.opennms.core.rpc.api.RpcResponse responseObj = mock(org.opennms.core.rpc.api.RpcResponse.class);
        when(module.execute(any())).thenReturn(CompletableFuture.completedFuture(responseObj));
        when(module.marshalResponse(responseObj)).thenReturn("response-payload");

        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<String, RpcModule> registry = Map.of("Echo", module);
        MinionIdentity identity = mock(MinionIdentity.class);
        when(identity.getId()).thenReturn("minion-A");
        when(identity.getLocation()).thenReturn("Default");

        @SuppressWarnings("unchecked")
        StreamObserver<RpcResponse> outbound = mock(StreamObserver.class);
        MinionRpcStreamClient client = new MinionRpcStreamClient(registry, identity, () -> outbound);

        StreamObserver<RpcRequest> inbound = client.streamObserver();
        RpcRequest req = RpcRequest.newBuilder()
            .setRpcId("xyz").setModuleId("Echo").setPayload(com.google.protobuf.ByteString.copyFromUtf8("p")).build();
        inbound.onNext(req);

        // Wait for async future
        try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        verify(outbound).onNext(argThat(r -> "xyz".equals(r.getRpcId())));
    }

    @Test
    void onUnknownModule_sendsErrorResponse() {
        Map<String, RpcModule> registry = Map.of();
        MinionIdentity identity = mock(MinionIdentity.class);
        when(identity.getId()).thenReturn("minion-A");
        when(identity.getLocation()).thenReturn("Default");

        @SuppressWarnings("unchecked")
        StreamObserver<RpcResponse> outbound = mock(StreamObserver.class);
        MinionRpcStreamClient client = new MinionRpcStreamClient(registry, identity, () -> outbound);

        StreamObserver<RpcRequest> inbound = client.streamObserver();
        RpcRequest req = RpcRequest.newBuilder().setRpcId("xyz").setModuleId("Unknown").build();
        inbound.onNext(req);

        verify(outbound).onNext(argThat(r ->
            "xyz".equals(r.getRpcId()) && r.getErrorMessage().contains("Unknown")));
    }
}
