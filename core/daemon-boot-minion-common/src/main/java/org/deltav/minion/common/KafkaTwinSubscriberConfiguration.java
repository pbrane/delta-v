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

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.opennms.core.ipc.twin.api.TwinSubscriber;
import org.opennms.core.ipc.twin.kafka.subscriber.KafkaTwinSubscriber;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.distributed.core.api.MinionIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;

/**
 * Spring Boot {@link Configuration} that wraps the OSGi-designed
 * {@link KafkaTwinSubscriber} in a Spring {@link SmartLifecycle}.
 *
 * <p>Lifecycle phase 100 initializes the Kafka Twin transport.
 * {@link MinionTwinBindingsConfiguration} runs at phase 200 to bind
 * subscribers like {@link org.opennms.minion.core.impl.PassiveStatusTwinSubscriber}
 * to whichever {@link TwinSubscriber} is active — same bindings work against
 * either this Kafka subscriber or the gRPC path's {@code LocalTwinSubscriberImpl}.
 *
 * <p>Active only when {@code opennms.minion.transport.twin=kafka} (Decision 4
 * sub-decision 4-ii's emergency rollback flag). The default {@code grpc} value
 * routes through {@link GrpcTwinStreamConfiguration} instead.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.twin.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(name = "opennms.minion.transport.twin", havingValue = "kafka")
public class KafkaTwinSubscriberConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTwinSubscriberConfiguration.class);

    @Bean
    public MetricRegistry minionTwinSubscriberMetricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge minionTwinSubscriberMetricsBridge(MetricRegistry minionTwinSubscriberMetricRegistry) {
        return new HorizonMetricsBridge(minionTwinSubscriberMetricRegistry, "opennms");
    }

    @Bean
    public KafkaTwinSubscriber kafkaTwinSubscriber(
            @Value("${opennms.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            MinionIdentity minionIdentity,
            TracerRegistry tracerRegistry,
            MetricRegistry minionTwinSubscriberMetricRegistry) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return new KafkaTwinSubscriber(
                minionIdentity,
                new SpringKafkaConfigProvider(kafkaProps),
                tracerRegistry,
                minionTwinSubscriberMetricRegistry);
    }

    @Bean
    public SmartLifecycle kafkaTwinSubscriberLifecycle(KafkaTwinSubscriber subscriber) {
        // Phase 100: initialize the Kafka transport. Subscriber binding (e.g.,
        // PassiveStatusTwinSubscriber.bind(...)) is now owned by
        // MinionTwinBindingsConfiguration's phase-200 lifecycle so the binding
        // logic is transport-agnostic — the same bind() works against either
        // KafkaTwinSubscriber (this bean) or LocalTwinSubscriberImpl from the
        // gRPC path. Both implement TwinSubscriber.
        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                LOG.info("Starting Kafka Twin subscriber (phase 100)");
                try {
                    subscriber.init();
                    running = true;
                    LOG.info("Kafka Twin subscriber started");
                } catch (Exception e) {
                    LOG.error("Failed to initialize Kafka Twin subscriber", e);
                }
            }

            @Override
            public void stop() {
                LOG.info("Stopping Kafka Twin subscriber (phase 100)");
                try {
                    subscriber.close();
                } catch (IOException e) {
                    LOG.warn("Error closing Kafka Twin subscriber", e);
                }
                running = false;
                LOG.info("Kafka Twin subscriber stopped");
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 100;
            }
        };
    }
}
