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

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.opennms.core.ipc.rpc.kafka.KafkaRpcServerManager;
import org.opennms.core.rpc.api.RpcModule;
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
 * {@link KafkaRpcServerManager} in a Spring {@link SmartLifecycle}.
 *
 * <p>Lifecycle phase 300 ensures the RPC server starts <b>after</b> the
 * Twin subscriber (phase 100) and the Sink client (phase 200). Shutdown
 * reverses this order automatically.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.rpc.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(name = "opennms.minion.transport.rpc", havingValue = "kafka")
public class KafkaRpcServerConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRpcServerConfiguration.class);

    @Bean
    public MetricRegistry minionRpcServerMetricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge minionRpcServerMetricsBridge(MetricRegistry minionRpcServerMetricRegistry) {
        return new HorizonMetricsBridge(minionRpcServerMetricRegistry, "opennms");
    }

    @Bean
    public KafkaRpcServerManager kafkaRpcServerManager(
            @Value("${opennms.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            MinionIdentity minionIdentity,
            TracerRegistry tracerRegistry,
            MetricRegistry minionRpcServerMetricRegistry) {

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        SpringKafkaConfigProvider configProvider = new SpringKafkaConfigProvider(kafkaProperties);

        return new KafkaRpcServerManager(configProvider, minionIdentity, tracerRegistry, minionRpcServerMetricRegistry);
    }

    @Bean
    public SmartLifecycle kafkaRpcServerLifecycle(
            KafkaRpcServerManager manager,
            RpcModuleRegistry rpcModuleRegistry) {

        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() {
                LOG.info("Starting Kafka RPC server (phase 300)");
                try {
                    manager.init();
                    for (String moduleId : rpcModuleRegistry.getModuleIds()) {
                        rpcModuleRegistry.getModule(moduleId).ifPresent(module -> {
                            try {
                                @SuppressWarnings("rawtypes")
                                RpcModule rawModule = module;
                                manager.bind(rawModule);
                                LOG.info("Bound RPC module: {}", moduleId);
                            } catch (Exception e) {
                                LOG.error("Failed to bind RPC module: {}", moduleId, e);
                            }
                        });
                    }
                    running = true;
                    LOG.info("Kafka RPC server started with {} module(s)", rpcModuleRegistry.getModuleIds().size());
                } catch (Exception e) {
                    LOG.error("Failed to initialize Kafka RPC server", e);
                }
            }

            @Override
            public void stop() {
                LOG.info("Stopping Kafka RPC server (phase 300)");
                for (String moduleId : rpcModuleRegistry.getModuleIds()) {
                    rpcModuleRegistry.getModule(moduleId).ifPresent(module -> {
                        try {
                            @SuppressWarnings("rawtypes")
                            RpcModule rawModule = module;
                            manager.unbind(rawModule);
                            LOG.info("Unbound RPC module: {}", moduleId);
                        } catch (Exception e) {
                            LOG.error("Failed to unbind RPC module: {}", moduleId, e);
                        }
                    });
                }
                manager.destroy();
                running = false;
                LOG.info("Kafka RPC server stopped");
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 300;
            }
        };
    }
}
