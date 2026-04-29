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
import io.grpc.stub.StreamObserver;
import org.deltav.gateway.twin.MinionTwinSubscriberRegistry;
import org.deltav.gateway.twin.TwinChannelDispatcher;
import org.deltav.minion.grpc.v1.SubscriptionMode;
import org.deltav.minion.grpc.v1.TwinSubscription;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.junit.jupiter.api.Test;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TwinChannelGrpcServiceTest {

    @Test
    void subscribe_registersAndTriggersInitialSnapshot() {
        MinionTwinSubscriberRegistry registry = mock(MinionTwinSubscriberRegistry.class);
        TwinChannelDispatcher dispatcher = mock(TwinChannelDispatcher.class);
        TwinChannelGrpcService svc = new TwinChannelGrpcService(registry, dispatcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> outbound = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<TwinSubscription> in = svc.channel(outbound);
            in.onNext(TwinSubscription.newBuilder()
                .setConsumerKey("passive-status")
                .setMode(SubscriptionMode.SUBSCRIBE).build());
        });

        verify(registry).register("passive-status", "Default", outbound);
        verify(dispatcher).sendInitialSnapshot("passive-status", "Default", outbound);
    }

    @Test
    void unsubscribe_removesFromRegistry() {
        MinionTwinSubscriberRegistry registry = mock(MinionTwinSubscriberRegistry.class);
        TwinChannelDispatcher dispatcher = mock(TwinChannelDispatcher.class);
        TwinChannelGrpcService svc = new TwinChannelGrpcService(registry, dispatcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> outbound = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<TwinSubscription> in = svc.channel(outbound);
            in.onNext(TwinSubscription.newBuilder()
                .setConsumerKey("passive-status")
                .setMode(SubscriptionMode.UNSUBSCRIBE).build());
        });

        verify(registry).unregister("passive-status", "Default", outbound);
    }

    @Test
    void streamClose_unregistersAllSubscriptions() {
        MinionTwinSubscriberRegistry registry = mock(MinionTwinSubscriberRegistry.class);
        TwinChannelDispatcher dispatcher = mock(TwinChannelDispatcher.class);
        TwinChannelGrpcService svc = new TwinChannelGrpcService(registry, dispatcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> outbound = mock(StreamObserver.class);

        Context ctx = Context.current()
            .withValue(MINION_ID_CTX, "minion-A")
            .withValue(MINION_LOCATION_CTX, "Default");
        ctx.run(() -> {
            StreamObserver<TwinSubscription> in = svc.channel(outbound);
            in.onCompleted();
        });

        verify(registry).unregisterAll(outbound);
    }
}
