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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Attaches Minion identity headers ({@code x-minion-id}, {@code x-minion-location})
 * to every outgoing gRPC call. minion-gateway routes Kafka publishes from these
 * metadata headers, not from the message payload.
 */
public class MinionIdentityClientInterceptor implements ClientInterceptor {

    public static final Metadata.Key<String> MINION_ID_HEADER =
        Metadata.Key.of("x-minion-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> MINION_LOCATION_HEADER =
        Metadata.Key.of("x-minion-location", Metadata.ASCII_STRING_MARSHALLER);

    private final String minionId;
    private final String location;

    public MinionIdentityClientInterceptor(String minionId, String location) {
        this.minionId = minionId;
        this.location = location;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(MINION_ID_HEADER, minionId);
                headers.put(MINION_LOCATION_HEADER, location);
                super.start(responseListener, headers);
            }
        };
    }
}
