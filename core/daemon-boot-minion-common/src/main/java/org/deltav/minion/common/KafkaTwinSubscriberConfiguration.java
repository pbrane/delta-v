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
import org.opennms.minion.core.impl.PassiveStatusTwinSubscriber;
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
 * <p>Lifecycle phase 100 ensures the Twin subscriber starts <b>first</b>,
 * before the Sink client (phase 200) and the RPC server (phase 300).
 * This allows config sync to complete before message dispatch and
 * request handling begin. Shutdown reverses this order automatically.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.twin.enabled", havingValue = "true", matchIfMissing = true)
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

    /**
     * Subscribes to passive service status updates from Pollerd via Twin API
     * so that {@link org.opennms.netmgt.poller.monitors.PassiveServiceMonitor}
     * can execute on Minion with current status data.
     *
     * <p>Binding happens in the lifecycle's {@code start()} after the Kafka
     * Twin subscriber is initialized, so the subscription is active.</p>
     */
    @Bean(destroyMethod = "close")
    public PassiveStatusTwinSubscriber passiveStatusTwinSubscriber() {
        return new PassiveStatusTwinSubscriber();
    }

    @Bean
    public SmartLifecycle kafkaTwinSubscriberLifecycle(
            KafkaTwinSubscriber subscriber,
            PassiveStatusTwinSubscriber passiveStatusTwinSubscriber) {

        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                LOG.info("Starting Kafka Twin subscriber (phase 100)");
                try {
                    subscriber.init();
                    passiveStatusTwinSubscriber.bind(subscriber);
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
