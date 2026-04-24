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
package org.deltav.netmgt.telemetry.boot;

import java.io.Closeable;
import java.util.function.Consumer;

import com.codahale.metrics.MetricRegistry;

import org.deltav.core.daemon.common.NoOpTracerRegistry;
import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.core.ipc.twin.api.LocalTwinSubscriber;
import org.opennms.core.ipc.twin.api.TwinPublisher;
import org.opennms.core.ipc.twin.api.TwinUpdate;
import org.opennms.core.ipc.twin.kafka.publisher.KafkaTwinPublisher;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.netmgt.dao.api.ServiceTracker;
import org.opennms.netmgt.telemetry.config.dao.TelemetrydConfigDao;
import org.opennms.netmgt.telemetry.daemon.ConnectorManager;
import org.opennms.netmgt.telemetry.daemon.LocationPublisherManager;
import org.opennms.netmgt.telemetry.daemon.OpenConfigTwinPublisher;
import org.opennms.netmgt.telemetry.daemon.OpenConfigTwinPublisherImpl;
import org.opennms.netmgt.telemetry.daemon.Telemetryd;
import org.opennms.netmgt.telemetry.protocols.registry.api.TelemetryServiceRegistry;
import org.opennms.netmgt.telemetry.protocols.registry.impl.TelemetryRegistryImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

/**
 * Spring Boot configuration for the Telemetryd daemon and its dependencies.
 *
 * <p>Wires the {@link Telemetryd} daemon with its configuration DAO, telemetry
 * registry (with no-op sub-registries since no adapters run locally), Twin API
 * chain (for ConnectorManager), and lifecycle management.</p>
 *
 * <p>All beans use constructor injection -- dependencies are passed explicitly
 * via {@code @Bean} factory method parameters.</p>
 */
@Configuration
public class TelemetrydDaemonConfiguration {


    @Value("${opennms.home:/opt/deltav}")
    private String opennmsHome;

    // ── 1. Configuration DAO ──────────────────────────────────────────

    @Bean
    public TelemetrydConfigDao telemetrydConfigDao() {
        var dao = new TelemetrydConfigDao();
        dao.setConfigResource(new FileSystemResource(opennmsHome + "/etc/telemetryd-configuration.xml"));
        return dao;
    }

    // ── 2. Telemetry Registry ─────────────────────────────────────────

    /**
     * No-op adapter registry. Telemetryd in this deployment is a pure ingestion
     * bridge -- no adapters execute locally.
     */
    @Bean
    @Qualifier("adapterRegistry")
    public TelemetryServiceRegistry<?, ?> adapterRegistry() {
        return noOpServiceRegistry();
    }

    @Bean
    @Qualifier("listenerRegistry")
    public TelemetryServiceRegistry<?, ?> listenerRegistry() {
        return noOpServiceRegistry();
    }

    @Bean
    @Qualifier("connectorRegistry")
    public TelemetryServiceRegistry<?, ?> connectorRegistry() {
        return noOpServiceRegistry();
    }

    @Bean
    @Qualifier("parserRegistry")
    public TelemetryServiceRegistry<?, ?> parserRegistry() {
        return noOpServiceRegistry();
    }

    @Bean
    public MetricRegistry metricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge telemetrydMetricsBridge(MetricRegistry metricRegistry) {
        return new HorizonMetricsBridge(metricRegistry, "opennms");
    }

    /**
     * The concrete TelemetryRegistry. The 4 sub-registries are wired via
     * {@code @Autowired @Qualifier} field injection inside {@link TelemetryRegistryImpl}.
     * MetricRegistry is wired via setter.
     */
    @Bean
    public TelemetryRegistryImpl telemetryRegistry(MetricRegistry metricRegistry) {
        var registry = new TelemetryRegistryImpl();
        registry.setMetricRegistry(metricRegistry);
        return registry;
    }

    // ── 3. ServiceTracker (no-op) ─────────────────────────────────────

    @Bean
    public ServiceTracker serviceTracker() {
        return new ServiceTracker() {
            @Override
            public Closeable trackServiceMatchingFilterRule(String serviceName, String filterRule, ServiceListener listener) {
                return () -> {};
            }

            @Override
            public Closeable trackService(String serviceName, ServiceListener listener) {
                return () -> {};
            }
        };
    }

    // ── 4. Twin API chain ─────────────────────────────────────────────

    // EntityScopeProvider is provided by daemon-common (DaemonProvisioningConfiguration)
    // TracerRegistry is provided by KafkaRpcClientConfiguration but only when RPC is enabled.
    // Telemetryd doesn't use RPC, so we provide our own NoOp.
    @Bean
    public TracerRegistry tracerRegistry() {
        return new NoOpTracerRegistry();
    }

    /**
     * No-op LocalTwinSubscriber. Telemetryd publishes Twin updates but does
     * not subscribe. KafkaTwinPublisher's constructor requires a non-null
     * LocalTwinSubscriber, so we provide a stub.
     */
    @Bean
    public LocalTwinSubscriber localTwinSubscriber(TracerRegistry tracerRegistry, MetricRegistry metricRegistry) {
        return new LocalTwinSubscriber() {
            @Override
            public void accept(TwinUpdate twinResponse) {
                // no-op
            }

            @Override
            public TracerRegistry getTracerRegistry() {
                return tracerRegistry;
            }

            @Override
            public MetricRegistry getMetricRegistry() {
                return metricRegistry;
            }

            @Override
            public <T> Closeable subscribe(String key, Class<T> clazz, Consumer<T> consumer) {
                return () -> {};
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    @Bean(initMethod = "init")
    public TwinPublisher twinPublisher(LocalTwinSubscriber localTwinSubscriber,
                                       TracerRegistry tracerRegistry,
                                       MetricRegistry metricRegistry) {
        return new KafkaTwinPublisher(localTwinSubscriber, tracerRegistry, metricRegistry);
    }

    @Bean
    public LocationPublisherManager locationPublisherManager(TwinPublisher twinPublisher) {
        return new LocationPublisherManager(twinPublisher);
    }

    @Bean
    public OpenConfigTwinPublisher openConfigTwinPublisher(LocationPublisherManager locationPublisherManager) {
        return new OpenConfigTwinPublisherImpl(locationPublisherManager);
    }

    // ── 5. ConnectorManager ───────────────────────────────────────────

    @Bean
    public ConnectorManager connectorManager(TelemetryRegistryImpl telemetryRegistry,
                                             EntityScopeProvider entityScopeProvider,
                                             ServiceTracker serviceTracker,
                                             OpenConfigTwinPublisher openConfigTwinPublisher) {
        return new ConnectorManager(telemetryRegistry, entityScopeProvider, serviceTracker, openConfigTwinPublisher);
    }

    // ── 6. Telemetryd daemon ──────────────────────────────────────────

    @Bean
    public Telemetryd telemetryd(TelemetrydConfigDao telemetrydConfigDao,
                                 MessageDispatcherFactory messageDispatcherFactory,
                                 MessageConsumerManager messageConsumerManager,
                                 ApplicationContext applicationContext,
                                 TelemetryRegistryImpl telemetryRegistry,
                                 ConnectorManager connectorManager) {
        return new Telemetryd(telemetrydConfigDao, messageDispatcherFactory, messageConsumerManager,
                applicationContext, telemetryRegistry, connectorManager, null);
    }

    // ── 7. SmartLifecycle ─────────────────────────────────────────────

    @Bean
    public SmartLifecycle telemetrydLifecycle(Telemetryd telemetryd) {
        return new SpringServiceDaemonSmartLifecycle(telemetryd, "Telemetryd");
    }

    // ── Helpers ───────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static <BD, T> TelemetryServiceRegistry<BD, T> noOpServiceRegistry() {
        return beanDefinition -> null;
    }
}
