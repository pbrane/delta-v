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
package org.deltav.minion.common;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.common.grpc.MinionIdentityClientInterceptor;
import org.deltav.minion.common.grpc.MinionTwinStreamClient;
import org.deltav.minion.grpc.v1.SubscriptionMode;
import org.deltav.minion.grpc.v1.TwinChannelServiceGrpc;
import org.deltav.minion.grpc.v1.TwinSubscription;
import org.opennms.core.ipc.twin.api.LocalTwinSubscriber;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Active when {@code opennms.minion.transport.twin=grpc} (default per
 * Decision 4 sub-decision 4-ii). Opens a bidi gRPC stream to minion-gateway
 * via the {@code minionGatewayChannel} bean (provided by
 * {@link GrpcHeartbeatDispatcherConfiguration}), sends one
 * {@link TwinSubscription} SUBSCRIBE per registered
 * {@link TwinSubscriptionRegistration} bean, and feeds inbound TwinUpdate
 * messages into {@link MinionTwinStreamClient} (Task 10).
 *
 * <p>SmartLifecycle phase 350 — between Sink phase 200 and RPC phase 400
 * (managed by {@link GrpcRpcStreamConfiguration}). Twin (config push) needs
 * to be ready before daemons that consume config (e.g. PassiveStatusTwin
 * subscribers); RPC server is last so it depends on already-running
 * infrastructure.
 *
 * <p>The {@link MinionTwinStreamClient} (constructed first as a {@code @Bean})
 * needs the lifecycle bean to call {@code stop()/start()} on version-gap
 * reconnect, but the lifecycle bean needs the client to obtain the inbound
 * {@link StreamObserver}. To break this cycle we introduce
 * {@link AtomicReference}{@code <SmartLifecycle>}: the client closure
 * dereferences it lazily (after both beans exist), and the
 * {@link #wireLifecycleRef} {@code @Autowired} setter populates the
 * reference once Spring has constructed both beans.
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.transport.twin", havingValue = "grpc", matchIfMissing = true)
public class GrpcTwinStreamConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcTwinStreamConfiguration.class);

    @Bean
    public AtomicReference<SmartLifecycle> twinStreamLifecycleRef() {
        return new AtomicReference<>();
    }

    @Bean
    public MinionTwinStreamClient minionTwinStreamClient(
            LocalTwinSubscriber localTwinSubscriber,
            AtomicReference<SmartLifecycle> twinStreamLifecycleRef) {
        // The reconnect trigger flips the lifecycle stop/start to drop the stream
        // and rebuild — Minion re-subscribes to all keys and receives fresh
        // snapshots per Decision 2 sub-decision 2-ii (revised).
        return new MinionTwinStreamClient(localTwinSubscriber, reason -> {
            SmartLifecycle lc = twinStreamLifecycleRef.get();
            if (lc != null && lc.isRunning()) {
                LOG.warn("Twin reconnect: {}", reason);
                lc.stop();
                lc.start();
            }
        });
    }

    /**
     * Lifecycle bean that opens the bidi gRPC stream at phase 350. The call
     * sequence in {@code start()} is:
     * <ol>
     *   <li>Build the {@link TwinChannelServiceGrpc} stub with the identity
     *       interceptor so outbound metadata carries x-minion-id /
     *       x-minion-location.</li>
     *   <li>Open the bidi stream via {@code stub.channel(client.streamObserver())},
     *       which returns the outbound observer used to send
     *       {@link TwinSubscription} messages.</li>
     *   <li>Iterate registered {@link TwinSubscriptionRegistration} beans and
     *       send a SUBSCRIBE per consumer key. The gateway responds with a
     *       fresh full snapshot per Decision 2 sub-decision 2-i.</li>
     * </ol>
     */
    @Bean
    public SmartLifecycle twinStreamLifecycle(
            @Qualifier("minionGatewayChannel") ManagedChannel channel,
            MinionIdentity identity,
            List<TwinSubscriptionRegistration> subscriptions,
            MinionTwinStreamClient client) {
        return new SmartLifecycle() {
            private volatile boolean running = false;
            private volatile StreamObserver<TwinSubscription> outbound;

            @Override
            public void start() {
                MinionIdentityClientInterceptor identityInterceptor =
                    new MinionIdentityClientInterceptor(identity.getId(), identity.getLocation());
                TwinChannelServiceGrpc.TwinChannelServiceStub stub =
                    TwinChannelServiceGrpc.newStub(channel).withInterceptors(identityInterceptor);
                outbound = stub.channel(client.streamObserver());

                for (TwinSubscriptionRegistration sub : subscriptions) {
                    outbound.onNext(TwinSubscription.newBuilder()
                        .setConsumerKey(sub.consumerKey())
                        .setMode(SubscriptionMode.SUBSCRIBE)
                        .build());
                }
                running = true;
                LOG.info("Twin stream opened to minion-gateway for minion={} location={} ({} subscriptions)",
                    identity.getId(), identity.getLocation(), subscriptions.size());
            }

            @Override
            public void stop() {
                if (outbound != null) {
                    try {
                        outbound.onCompleted();
                    } catch (Throwable t) {
                        // best-effort close; the lifecycle is being torn down
                        LOG.debug("Twin outbound close threw during stop (best-effort): {}", t.toString());
                    }
                    outbound = null;
                }
                running = false;
                LOG.info("Twin stream closed for minion={}", identity.getId());
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 350;
            }
        };
    }

    /**
     * Daemon-boot modules register one of these per consumer_key they want
     * subscribed (e.g. {@code new TwinSubscriptionRegistration("passive-status")}).
     * Spring auto-collects all such beans into the {@code List<>} the
     * lifecycle bean iterates in {@code start()}.
     *
     * <p>Example registration in a daemon-boot module's @Configuration:
     * <pre>{@code
     * @Bean
     * TwinSubscriptionRegistration passiveStatusSubscription() {
     *     return new TwinSubscriptionRegistration("passive-status");
     * }
     * }</pre>
     *
     * <p>If no daemon registers any of these, the lifecycle still opens the
     * stream but sends zero SUBSCRIBE messages — Minion gets no Twin state
     * over gRPC. This is the rollout path: existing Kafka subscribers keep
     * consuming via horizon's path until daemons migrate to register
     * {@link TwinSubscriptionRegistration} beans.
     */
    public record TwinSubscriptionRegistration(String consumerKey) {}

    /**
     * Populates the AtomicReference after Spring has constructed both the
     * client and the lifecycle bean — breaks the bidirectional dep so the
     * reconnect trigger captured inside the client closure can resolve the
     * lifecycle bean lazily.
     */
    @Autowired
    public void wireLifecycleRef(
            AtomicReference<SmartLifecycle> twinStreamLifecycleRef,
            @Qualifier("twinStreamLifecycle") SmartLifecycle twinStreamLifecycle) {
        twinStreamLifecycleRef.set(twinStreamLifecycle);
    }
}
