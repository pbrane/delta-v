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
package org.deltav.netmgt.discovery.boot;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import io.micrometer.core.instrument.MeterRegistry;

import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.deltav.core.daemon.common.JdbcDistPollerDao;
import org.deltav.core.daemon.common.JdbcInterfaceToNodeCache;
import org.deltav.core.daemon.registry.DetectorRegistryConfiguration;
import org.deltav.core.daemon.registry.NoOpSnmpAgentConfigFactory;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.DiscoveryConfigFactory;
import org.opennms.netmgt.config.api.DiscoveryConfigurationFactory;
import org.opennms.netmgt.config.discovery.DiscoveryConfiguration;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.discovery.Discovery;
import org.opennms.netmgt.discovery.DiscoveryTaskExecutorImpl;
import org.opennms.netmgt.discovery.RangeChunker;
import org.opennms.netmgt.discovery.UnmanagedInterfaceFilter;
import org.opennms.netmgt.icmp.best.BestMatchPingerFactory;
import org.opennms.netmgt.icmp.PingerFactory;
import org.opennms.core.rpc.api.RpcClientFactory;
import org.opennms.netmgt.icmp.proxy.LocationAwarePingClient;
import org.opennms.netmgt.icmp.proxy.LocationAwarePingClientImpl;
import org.opennms.netmgt.icmp.proxy.PingProxyRpcModule;
import org.opennms.netmgt.icmp.proxy.PingSweepRpcModule;
import org.opennms.netmgt.provision.LocationAwareDetectorClient;
import org.opennms.netmgt.provision.detector.client.rpc.DetectorClientRpcModule;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.provision.detector.client.rpc.LocationAwareDetectorClientRpcImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Spring Boot @Configuration that wires all Discovery beans.
 *
 * <p>Replaces the Karaf-era {@code applicationContext-daemon-loader-discovery.xml}.</p>
 *
 * <p>{@link DetectorRegistryConfiguration} is imported explicitly to provide
 * the {@link org.opennms.netmgt.provision.detector.registry.api.ServiceDetectorRegistry}
 * bean that {@link DetectorClientRpcModule} requires via {@code @Autowired}.
 * Discovery doesn't need the collector or monitor registries, so we import
 * only the detector configuration — the same pattern Minion uses.</p>
 */
@Configuration
@Import(DetectorRegistryConfiguration.class)
public class DiscoveryBootConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryBootConfiguration.class);

    private static final XmlMapper XML_MAPPER;
    static {
        XML_MAPPER = XmlMapper.builder()
                .defaultUseWrapper(false)
                .build();
        XML_MAPPER.registerModule(new JaxbAnnotationModule());
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    // -- SNMP Config (NoOp — detectors execute on Minion via RPC) --

    @Bean
    public SnmpAgentConfigFactory snmpAgentConfigFactory() {
        return new NoOpSnmpAgentConfigFactory();
    }

    // -- DAO / Cache --

    @Bean
    public DistPollerDao distPollerDao(DataSource dataSource) {
        return new JdbcDistPollerDao(dataSource);
    }

    @Bean
    public InterfaceToNodeCache interfaceToNodeCache(DataSource dataSource) {
        return new JdbcInterfaceToNodeCache(dataSource);
    }

    // -- Discovery Config --

    /**
     * Reads discovery-configuration.xml via Jackson XmlMapper, then wraps the
     * deserialized model in {@link DiscoveryConfigFactory} for backward
     * compatibility with feature-module code that still references the concrete
     * class (RangeChunker, DiscoveryTaskExecutorImpl).
     */
    @Bean
    public DiscoveryConfigurationFactory discoveryConfigFactory(
            @Value("${opennms.home}") String opennmsHome) throws IOException {
        var configFile = new File(opennmsHome, "etc/discovery-configuration.xml");
        if (!configFile.exists()) {
            throw new IOException("discovery-configuration.xml not found at " + configFile);
        }
        LOG.info("Loading discovery configuration from {}", configFile);
        var model = XML_MAPPER.readValue(configFile, DiscoveryConfiguration.class);
        return new DiscoveryConfigFactory(model);
    }

    // -- ICMP Ping RPC --

    @Bean
    public PingerFactory pingerFactory() {
        return new BestMatchPingerFactory();
    }

    @Bean
    public PingProxyRpcModule pingProxyRpcModule() {
        return new PingProxyRpcModule();
    }

    @Bean
    public PingSweepRpcModule pingSweepRpcModule() {
        return new PingSweepRpcModule();
    }

    /**
     * IMPORTANT: Uses {@code javax.annotation.PostConstruct} which Spring 7
     * does NOT recognize. Must use {@code initMethod = "init"}.
     */
    @Bean(initMethod = "init")
    public LocationAwarePingClientImpl locationAwarePingClient(
            RpcClientFactory rpcClientFactory,
            PingProxyRpcModule pingProxyRpcModule,
            PingSweepRpcModule pingSweepRpcModule) {
        return new LocationAwarePingClientImpl(rpcClientFactory, pingProxyRpcModule, pingSweepRpcModule);
    }

    // -- Detector RPC --

    @Bean(name = "scanExecutor")
    public Executor scanExecutor() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    public DetectorClientRpcModule detectorClientRpcModule() {
        return new DetectorClientRpcModule();
    }

    @Bean
    public LocationAwareDetectorClient locationAwareDetectorClient() {
        // Implements InitializingBean — Spring 7 calls afterPropertiesSet() natively
        return new LocationAwareDetectorClientRpcImpl();
    }

    // -- Discovery Core --

    @Bean
    public UnmanagedInterfaceFilter unmanagedInterfaceFilter(InterfaceToNodeCache cache) {
        return new UnmanagedInterfaceFilter(cache);
    }

    @Bean
    public RangeChunker rangeChunker(UnmanagedInterfaceFilter filter) {
        return new RangeChunker(filter);
    }

    /**
     * Wraps the discovery daemon's {@link EventForwarder} so that every event
     * forwarded — and {@code newSuspect} events specifically — increments
     * Micrometer counters at {@code /actuator/prometheus}. Suspects-found
     * is the operationally meaningful "discovery is doing real work" signal.
     */
    @Bean(name = "countingDiscoveryEventForwarder")
    public EventForwarder countingDiscoveryEventForwarder(
            @Qualifier("eventIpcManager") EventForwarder eventForwarder,
            MeterRegistry meterRegistry) {
        return new CountingEventForwarder(eventForwarder, meterRegistry);
    }

    @Bean
    public DiscoveryTaskExecutorImpl discoveryTaskExecutor(
            RangeChunker rangeChunker,
            LocationAwarePingClient locationAwarePingClient,
            @Qualifier("countingDiscoveryEventForwarder") EventForwarder eventForwarder,
            MeterRegistry meterRegistry) {
        return new CountingDiscoveryTaskExecutor(rangeChunker, locationAwarePingClient,
                eventForwarder, null, meterRegistry);
    }

    @Bean
    public Discovery discovery(DiscoveryConfigurationFactory discoveryConfigFactory,
                               DiscoveryTaskExecutorImpl discoveryTaskExecutor,
                               @Qualifier("countingDiscoveryEventForwarder") EventForwarder eventForwarder) {
        return new Discovery(discoveryConfigFactory, discoveryTaskExecutor, eventForwarder);
    }

    @Bean
    public SmartLifecycle discoveryLifecycle(Discovery discovery) {
        return new SpringServiceDaemonSmartLifecycle(discovery);
    }

    // Note: Config reload via RELOAD_DAEMON_CONFIG_UEI events is NOT wired.
    // Discovery.reloadAndReStart() is private with no @EventHandler annotation.
    // To reload config, restart the container. This is acceptable for the
    // initial migration; event-driven reload can be added later.
}
