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
package org.deltav.core.daemon.common;

import com.codahale.metrics.MetricRegistry;

import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.opennms.core.ipc.rpc.kafka.KafkaRpcClientFactory;
import org.opennms.core.rpc.utils.RpcTargetHelper;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring {@link Configuration} for the Kafka RPC client infrastructure.
 *
 * <p>Shared by all daemons that send Kafka RPC requests to Minions:
 * Discovery, Pollerd, Collectd, Enlinkd, PerspectivePoller, Provisiond.</p>
 *
 * <p>Bridges Spring properties to system properties for legacy
 * {@code KafkaRpcClientFactory} which reads configuration via
 * {@code OnmsKafkaConfigProvider} (system property scan) and
 * {@code Boolean.getBoolean()} calls.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.rpc.kafka.enabled", havingValue = "true", matchIfMissing = false)
public class KafkaRpcClientConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRpcClientConfiguration.class);

    @Value("${opennms.rpc.kafka.bootstrap-servers:kafka:9092}")
    private String rpcBootstrapServers;

    @Value("${opennms.rpc.kafka.force-remote:true}")
    private String forceRemote;

    @Bean
    public TracerRegistry tracerRegistry() {
        return new NoOpTracerRegistry();
    }

    @Bean
    public MetricRegistry kafkaRpcMetricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge kafkaRpcMetricsBridge(MetricRegistry kafkaRpcMetricRegistry) {
        return new HorizonMetricsBridge(kafkaRpcMetricRegistry, "opennms");
    }

    @Bean
    public RpcTargetHelper rpcTargetHelper() {
        return new RpcTargetHelper();
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(name = "opennms.rpc.kafka.enabled", havingValue = "true", matchIfMissing = false)
    public KafkaRpcClientFactory rpcClientFactory(DistPollerDao distPollerDao,
                                                   MetricRegistry kafkaRpcMetricRegistry) {
        System.setProperty("org.opennms.core.ipc.rpc.kafka.bootstrap.servers", rpcBootstrapServers);
        System.setProperty("org.opennms.core.ipc.rpc.force-remote", forceRemote);
        LOG.info("Bridged RPC Kafka bootstrap.servers={}, force-remote={}", rpcBootstrapServers, forceRemote);

        var factory = new KafkaRpcClientFactory();
        factory.setLocation(distPollerDao.whoami().getLocation());
        factory.setMetrics(kafkaRpcMetricRegistry);
        return factory;
    }
}
