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
package org.deltav.gateway.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.stereotype.Component;

/**
 * Server-side interceptor that pulls Minion identity out of gRPC metadata
 * (x-minion-id, x-minion-location) and stashes it in the gRPC Context so
 * service handlers can read it without seeing the metadata layer.
 *
 * <p>Annotated with {@link GlobalServerInterceptor} so Spring gRPC's
 * auto-config applies it to every {@code @GrpcService}.
 */
@Component
@GlobalServerInterceptor
public class MinionIdentityServerInterceptor implements ServerInterceptor {

    public static final Metadata.Key<String> MINION_ID_HEADER =
        Metadata.Key.of("x-minion-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> MINION_LOCATION_HEADER =
        Metadata.Key.of("x-minion-location", Metadata.ASCII_STRING_MARSHALLER);

    public static final Context.Key<String> MINION_ID_CTX = Context.key("minion-id");
    public static final Context.Key<String> MINION_LOCATION_CTX = Context.key("minion-location");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String id = headers.get(MINION_ID_HEADER);
        String location = headers.get(MINION_LOCATION_HEADER);

        if (id == null || id.isBlank() || location == null || location.isBlank()) {
            call.close(Status.UNAUTHENTICATED.withDescription(
                "x-minion-id and x-minion-location metadata required"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, id)
            .withValue(MINION_LOCATION_CTX, location);
        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
