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

import io.grpc.stub.StreamObserver;
import org.deltav.gateway.twin.MinionTwinSubscriberRegistry;
import org.deltav.gateway.twin.TwinChannelDispatcher;
import org.deltav.minion.grpc.v1.SubscriptionMode;
import org.deltav.minion.grpc.v1.TwinChannelServiceGrpc;
import org.deltav.minion.grpc.v1.TwinSubscription;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_ID_CTX;
import static org.deltav.gateway.grpc.MinionIdentityServerInterceptor.MINION_LOCATION_CTX;

/**
 * Bidi-streaming gRPC service for Twin updates. Per Decision 2 of the
 * v1.2.0-rc2 decisions doc:
 * - On TwinSubscription(SUBSCRIBE): register in MinionTwinSubscriberRegistry,
 *   fire initial snapshot via TwinChannelDispatcher.
 * - On TwinSubscription(UNSUBSCRIBE): unregister.
 * - On stream close (onCompleted/onError): unregister all subscriptions.
 *
 * <p>Sequencing on subscribe: register-before-snapshot is intentional. If a
 * Kafka update for the same (consumer_key, location) arrives between register
 * and the initial snapshot send, the dispatcher's broadcast logic will see
 * the new subscriber and send the broadcast — possibly before the snapshot.
 * The Minion-side subscriber tolerates this by treating the higher-version
 * message as authoritative; an out-of-order older snapshot is dropped on the
 * version-gap check (Decision 2 sub-decision 2-ii).
 */
@GrpcService
public class TwinChannelGrpcService extends TwinChannelServiceGrpc.TwinChannelServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TwinChannelGrpcService.class);

    private final MinionTwinSubscriberRegistry registry;
    private final TwinChannelDispatcher dispatcher;

    public TwinChannelGrpcService(MinionTwinSubscriberRegistry registry, TwinChannelDispatcher dispatcher) {
        this.registry = registry;
        this.dispatcher = dispatcher;
    }

    @Override
    public StreamObserver<TwinSubscription> channel(StreamObserver<TwinUpdate> outbound) {
        String minionId = MINION_ID_CTX.get();
        String location = MINION_LOCATION_CTX.get();
        LOG.info("Twin stream opened for minion={} location={}", minionId, location);

        return new StreamObserver<>() {
            @Override
            public void onNext(TwinSubscription req) {
                String key = req.getConsumerKey();
                if (req.getMode() == SubscriptionMode.SUBSCRIBE) {
                    LOG.info("Twin SUBSCRIBE minion={} location={} key={}", minionId, location, key);
                    registry.register(key, location, outbound);
                    dispatcher.sendInitialSnapshot(key, location, outbound);
                } else {
                    LOG.info("Twin UNSUBSCRIBE minion={} location={} key={}", minionId, location, key);
                    registry.unregister(key, location, outbound);
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.info("Twin stream error for minion={}: {}", minionId, t.toString());
                registry.unregisterAll(outbound);
            }

            @Override
            public void onCompleted() {
                LOG.info("Twin stream closed for minion={}", minionId);
                registry.unregisterAll(outbound);
                outbound.onCompleted();
            }
        };
    }
}
