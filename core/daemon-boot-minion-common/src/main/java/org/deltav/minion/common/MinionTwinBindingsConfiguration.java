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

import org.opennms.core.ipc.twin.api.TwinSubscriber;
import org.opennms.minion.core.impl.PassiveStatusTwinSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Transport-agnostic Spring configuration for Minion-side Twin subscribers.
 *
 * <p>Active when {@code opennms.minion.twin.enabled=true} (default), regardless
 * of {@code opennms.minion.transport.twin} value. Owns the per-Minion subscriber
 * beans (e.g., {@link PassiveStatusTwinSubscriber}) and the
 * {@link GrpcTwinStreamConfiguration.TwinSubscriptionRegistration} marker beans
 * that the gRPC stream uses to know which consumer keys to SUBSCRIBE to.
 *
 * <p>The phase-200 {@link SmartLifecycle} binds each subscriber to the
 * transport-active {@link TwinSubscriber} bean (provided by either
 * {@link KafkaTwinSubscriberConfiguration} at phase 100 in Kafka mode or
 * {@link GrpcTwinStreamConfiguration} at bean-construction time in gRPC mode).
 * Binding registers a local callback on the subscriber's
 * {@code AbstractTwinSubscriber.subscribe(...)} dispatch table — when
 * {@code accept(TwinUpdate)} is called by the transport layer, the registered
 * callback fires.
 *
 * <p>Phase ordering:
 * <ul>
 *   <li>Phase 100: KafkaTwinSubscriber.init() (Kafka mode only)</li>
 *   <li>Phase 200: this lifecycle — passiveStatusTwinSubscriber.bind(twinSubscriber)</li>
 *   <li>Phase 350: GrpcTwinStreamConfiguration twinStreamLifecycle (gRPC mode only)
 *       — opens bidi stream and sends SUBSCRIBE per
 *       {@link GrpcTwinStreamConfiguration.TwinSubscriptionRegistration} bean</li>
 * </ul>
 *
 * <p>In gRPC mode, the bindings register callbacks on the
 * {@code LocalTwinSubscriberImpl} bean. When the gRPC stream delivers a
 * TwinUpdate, {@code MinionTwinStreamClient} calls
 * {@code localTwinSubscriber.accept(update)} which fires the registered
 * callbacks. This decouples the transport-specific wire reader from the
 * Minion-internal subscriber dispatch.
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.twin.enabled", havingValue = "true", matchIfMissing = true)
public class MinionTwinBindingsConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(MinionTwinBindingsConfiguration.class);

    @Bean(destroyMethod = "close")
    public PassiveStatusTwinSubscriber passiveStatusTwinSubscriber() {
        return new PassiveStatusTwinSubscriber();
    }

    /**
     * Marker bean consumed by the gRPC stream lifecycle (Task 11) to send a
     * SUBSCRIBE for the {@code passive-status} consumer key. Has no effect in
     * Kafka mode (the GrpcTwinStreamConfiguration that consumes the list is
     * conditional-off; the bean is created but unused).
     */
    @Bean
    public GrpcTwinStreamConfiguration.TwinSubscriptionRegistration passiveStatusTwinSubscriptionRegistration() {
        return new GrpcTwinStreamConfiguration.TwinSubscriptionRegistration("passive-status");
    }

    @Bean
    public SmartLifecycle minionTwinBindingsLifecycle(TwinSubscriber twinSubscriber,
                                                      PassiveStatusTwinSubscriber passiveStatusTwinSubscriber) {
        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                LOG.info("Binding Minion-side Twin subscribers to transport (phase 200)");
                passiveStatusTwinSubscriber.bind(twinSubscriber);
                running = true;
            }

            @Override
            public void stop() {
                LOG.info("Unbinding Minion-side Twin subscribers (phase 200)");
                try {
                    passiveStatusTwinSubscriber.unbind(twinSubscriber);
                } catch (Throwable t) {
                    LOG.debug("PassiveStatusTwinSubscriber.unbind threw during stop (best-effort): {}", t.toString());
                }
                running = false;
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 200;
            }
        };
    }
}
