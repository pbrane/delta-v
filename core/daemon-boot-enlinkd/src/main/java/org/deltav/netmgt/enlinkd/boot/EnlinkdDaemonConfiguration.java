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
package org.deltav.netmgt.enlinkd.boot;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import io.micrometer.core.instrument.MeterRegistry;

import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.deltav.netmgt.enlinkd.persistence.cache.TopologyEntityCacheImpl;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.rpc.api.RpcClientFactory;
import org.opennms.netmgt.config.EnhancedLinkdConfig;
import org.opennms.netmgt.config.EnhancedLinkdConfigFactory;
import org.opennms.netmgt.config.SnmpPeerFactory;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.snmp.SnmpConfig;
import org.opennms.netmgt.dao.api.IpInterfaceDao;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.enlinkd.BridgeOnmsTopologyUpdater;
import org.opennms.netmgt.enlinkd.CdpOnmsTopologyUpdater;
import org.opennms.netmgt.enlinkd.DiscoveryBridgeDomains;
import org.opennms.netmgt.enlinkd.EnhancedLinkd;
import org.opennms.netmgt.enlinkd.EventProcessor;
import org.opennms.netmgt.enlinkd.TopologyUpdaterRegistry;
import org.opennms.netmgt.enlinkd.IsisOnmsTopologyUpdater;
import org.opennms.netmgt.enlinkd.LldpOnmsTopologyUpdater;
import org.opennms.netmgt.enlinkd.NetworkRouterTopologyUpdater;
import org.opennms.netmgt.enlinkd.NodesOnmsTopologyUpdater;
import org.opennms.netmgt.enlinkd.OspfAreaOnmsTopologyUpdater;
import org.opennms.netmgt.enlinkd.OspfOnmsTopologyUpdater;
import org.opennms.netmgt.enlinkd.UserDefinedLinkTopologyUpdater;
import org.opennms.netmgt.enlinkd.persistence.impl.BridgeBridgeLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.BridgeElementDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.BridgeMacLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.BridgeStpLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.CdpElementDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.CdpLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.IpNetToMediaDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.IsIsElementDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.IsIsLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.LldpElementDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.LldpLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.OspfAreaDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.OspfElementDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.OspfLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.UserDefinedLinkDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityCache;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;
import org.opennms.netmgt.enlinkd.service.api.BridgeTopologyService;
import org.opennms.netmgt.enlinkd.service.api.CdpTopologyService;
import org.opennms.netmgt.enlinkd.service.api.IpNetToMediaTopologyService;
import org.opennms.netmgt.enlinkd.service.api.IsisTopologyService;
import org.opennms.netmgt.enlinkd.service.api.LldpTopologyService;
import org.opennms.netmgt.enlinkd.service.api.NodeTopologyService;
import org.opennms.netmgt.enlinkd.service.api.ProtocolSupported;
import org.opennms.netmgt.enlinkd.service.api.OspfTopologyService;
import org.opennms.netmgt.enlinkd.service.api.UserDefinedLinkTopologyService;
import org.opennms.netmgt.enlinkd.service.impl.BridgeTopologyServiceImpl;
import org.opennms.netmgt.enlinkd.service.impl.CdpTopologyServiceImpl;
import org.opennms.netmgt.enlinkd.service.impl.IpNetToMediaTopologyServiceImpl;
import org.opennms.netmgt.enlinkd.service.impl.IsisTopologyServiceImpl;
import org.opennms.netmgt.enlinkd.service.impl.LldpTopologyServiceImpl;
import org.opennms.netmgt.enlinkd.service.impl.NodeTopologyServiceImpl;
import org.opennms.netmgt.enlinkd.service.impl.OspfTopologyServiceImpl;
import org.opennms.netmgt.enlinkd.service.impl.UserDefinedLinkTopologyServiceImpl;
import org.opennms.netmgt.events.api.AnnotationBasedEventListenerAdapter;
import org.opennms.netmgt.events.api.EventSubscriptionService;
import org.opennms.netmgt.snmp.proxy.LocationAwareSnmpClient;
import org.opennms.netmgt.snmp.proxy.common.LocationAwareSnmpClientRpcImpl;
import org.opennms.netmgt.topologies.service.api.OnmsTopologyDao;
import org.opennms.netmgt.topologies.service.impl.OnmsTopologyDaoInMemoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot configuration for the Enlinkd daemon and its dependencies.
 *
 * <p>Wires the {@link EnhancedLinkd} daemon with topology services, updaters,
 * SNMP client, event processor, and lifecycle management.</p>
 *
 * <p>JPA DAOs in {@code opennms-model-jakarta} implement the persistence API
 * interfaces (same FQCN as legacy entities), so topology services receive
 * DAOs via their normal typed setter methods.</p>
 */
@Configuration
public class EnlinkdDaemonConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(EnlinkdDaemonConfiguration.class);

    private static final XmlMapper XML_MAPPER;
    static {
        XML_MAPPER = XmlMapper.builder().defaultUseWrapper(false).build();
        XML_MAPPER.registerModule(new JaxbAnnotationModule());
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Value("${opennms.home:/opt/deltav}")
    private String opennmsHome;

    // ── 1. Infrastructure ────────────────────────────────────────────

    // ── 2. SNMP ──────────────────────────────────────────────────────

    @Bean
    public SnmpAgentConfigFactory snmpPeerFactory(EntityScopeProvider entityScopeProvider) throws IOException {
        var configFile = new File(opennmsHome, "etc/snmp-config.xml");
        LOG.info("Loading SnmpPeerFactory from {}", configFile);
        var config = XML_MAPPER.readValue(configFile, SnmpConfig.class);
        var factory = new SnmpPeerFactory(config, entityScopeProvider, null);
        SnmpPeerFactory.setInstance(factory);
        return factory;
    }

    @Bean
    public LocationAwareSnmpClient locationAwareSnmpClient(RpcClientFactory rpcClientFactory) {
        return new LocationAwareSnmpClientRpcImpl(rpcClientFactory);
    }

    // ── 3. Enlinkd Config ────────────────────────────────────────────

    @Bean
    public EnhancedLinkdConfig linkdConfig() throws IOException {
        LOG.info("Initializing EnhancedLinkdConfigFactory");
        return new EnhancedLinkdConfigFactory();
    }

    // ── 4. OnmsTopologyDao ───────────────────────────────────────────

    /**
     * Wraps the in-memory OnmsTopologyDao with a Micrometer-counting
     * decorator so every protocol updater's {@code update(...)} call is
     * surfaced at {@code /actuator/prometheus} as
     * {@code deltav_enlinkd_topology_updates_total{protocol,status}}.
     */
    @Bean
    public OnmsTopologyDao onmsTopologyDao(MeterRegistry meterRegistry) {
        return new CountingOnmsTopologyDao(new OnmsTopologyDaoInMemoryImpl(), meterRegistry);
    }

    // ── 5. TopologyEntityCache (real JPA-backed) ─────────────────────

    /**
     * Guava-backed cache wrapping the {@link TopologyEntityDao} provided by
     * {@link EnlinkdJpaConfiguration}. TTL is set via
     * {@code deltav.enlinkd.topology-cache.duration-seconds} (default 300s,
     * matching horizon's behavior).
     */
    @Bean
    public TopologyEntityCache topologyEntityCache(
            TopologyEntityDao topologyEntityDao,
            @Value("${deltav.enlinkd.topology-cache.duration-seconds:300}") int cacheDurationSeconds) {
        return new TopologyEntityCacheImpl(topologyEntityDao, cacheDurationSeconds);
    }

    // ── 6. Topology Services ─────────────────────────────────────────
    //
    // Our JPA DAOs now implement the persistence API interfaces, so normal
    // setter injection works — no more reflective field injection needed.

    @Bean
    public NodeTopologyService nodeTopologyService(NodeDao nodeDao,
                                                    TopologyEntityCache topologyEntityCache) {
        var svc = new NodeTopologyServiceImpl();
        svc.setNodeDao(nodeDao);
        svc.setTopologyEntityCache(topologyEntityCache);
        return svc;
    }

    @Bean
    public CdpTopologyService cdpTopologyService(PlatformTransactionManager transactionManager,
                                                  CdpLinkDaoJpa cdpLinkDao,
                                                  CdpElementDaoJpa cdpElementDao,
                                                  TopologyEntityCache topologyEntityCache) {
        var svc = new CdpTopologyServiceImpl(transactionManager);
        svc.setCdpLinkDao(cdpLinkDao);
        svc.setCdpElementDao(cdpElementDao);
        svc.setTopologyEntityCache(topologyEntityCache);
        return svc;
    }

    @Bean
    public LldpTopologyService lldpTopologyService(PlatformTransactionManager transactionManager,
                                                    LldpLinkDaoJpa lldpLinkDao,
                                                    LldpElementDaoJpa lldpElementDao,
                                                    TopologyEntityCache topologyEntityCache) {
        var svc = new LldpTopologyServiceImpl(transactionManager);
        svc.setLldpLinkDao(lldpLinkDao);
        svc.setLldpElementDao(lldpElementDao);
        svc.setTopologyEntityCache(topologyEntityCache);
        return svc;
    }

    @Bean
    public OspfTopologyService ospfTopologyService(PlatformTransactionManager transactionManager,
                                                    OspfLinkDaoJpa ospfLinkDao,
                                                    OspfElementDaoJpa ospfElementDao,
                                                    OspfAreaDaoJpa ospfAreaDao,
                                                    TopologyEntityCache topologyEntityCache) {
        var svc = new OspfTopologyServiceImpl(transactionManager);
        svc.setOspfLinkDao(ospfLinkDao);
        svc.setOspfElementDao(ospfElementDao);
        svc.setOspfAreaDao(ospfAreaDao);
        svc.setTopologyEntityCache(topologyEntityCache);
        return svc;
    }

    @Bean
    public IsisTopologyService isisTopologyService(PlatformTransactionManager transactionManager,
                                                    IsIsLinkDaoJpa isisLinkDao,
                                                    IsIsElementDaoJpa isisElementDao,
                                                    TopologyEntityCache topologyEntityCache) {
        var svc = new IsisTopologyServiceImpl(transactionManager);
        svc.setIsisLinkDao(isisLinkDao);
        svc.setIsisElementDao(isisElementDao);
        svc.setTopologyEntityCache(topologyEntityCache);
        return svc;
    }

    @Bean
    public BridgeTopologyService bridgeTopologyService(PlatformTransactionManager transactionManager,
                                                        BridgeElementDaoJpa bridgeElementDao,
                                                        BridgeBridgeLinkDaoJpa bridgeBridgeLinkDao,
                                                        BridgeMacLinkDaoJpa bridgeMacLinkDao,
                                                        BridgeStpLinkDaoJpa bridgeStpLinkDao,
                                                        IpNetToMediaDaoJpa ipNetToMediaDao,
                                                        TopologyEntityCache topologyEntityCache) {
        var svc = new BridgeTopologyServiceImpl(transactionManager);
        svc.setBridgeElementDao(bridgeElementDao);
        svc.setBridgeBridgeLinkDao(bridgeBridgeLinkDao);
        svc.setBridgeMacLinkDao(bridgeMacLinkDao);
        svc.setBridgeStpLinkDao(bridgeStpLinkDao);
        svc.setIpNetToMediaDao(ipNetToMediaDao);
        svc.setTopologyEntityCache(topologyEntityCache);
        return svc;
    }

    @Bean
    public IpNetToMediaTopologyService ipNetToMediaTopologyService(PlatformTransactionManager transactionManager,
                                                                    IpNetToMediaDaoJpa ipNetToMediaDao,
                                                                    IpInterfaceDao ipInterfaceDao) {
        var svc = new IpNetToMediaTopologyServiceImpl(transactionManager);
        svc.setIpNetToMediaDao(ipNetToMediaDao);
        svc.setIpInterfaceDao(ipInterfaceDao);
        return svc;
    }

    @Bean
    public UserDefinedLinkTopologyService userDefinedLinkTopologyService(
            UserDefinedLinkDaoJpa userDefinedLinkDao,
            TopologyEntityCache topologyEntityCache) {
        var svc = new UserDefinedLinkTopologyServiceImpl();
        svc.setUserDefinedLinkDao(userDefinedLinkDao);
        svc.setTopologyEntityCache(topologyEntityCache);
        return svc;
    }

    // ── 7. Topology Updaters ─────────────────────────────────────────

    @Bean
    public NodesOnmsTopologyUpdater nodesTopologyUpdater(OnmsTopologyDao topologyDao,
                                                          NodeTopologyService nodeTopologyService) {
        return new NodesOnmsTopologyUpdater(topologyDao, nodeTopologyService);
    }

    @Bean
    public CdpOnmsTopologyUpdater cdpTopologyUpdater(OnmsTopologyDao topologyDao,
                                                      CdpTopologyService cdpTopologyService,
                                                      NodeTopologyService nodeTopologyService) {
        return new CdpOnmsTopologyUpdater(topologyDao, cdpTopologyService, nodeTopologyService);
    }

    @Bean
    public LldpOnmsTopologyUpdater lldpTopologyUpdater(OnmsTopologyDao topologyDao,
                                                        LldpTopologyService lldpTopologyService,
                                                        NodeTopologyService nodeTopologyService) {
        return new LldpOnmsTopologyUpdater(topologyDao, lldpTopologyService, nodeTopologyService);
    }

    @Bean
    public IsisOnmsTopologyUpdater isisTopologyUpdater(OnmsTopologyDao topologyDao,
                                                        IsisTopologyService isisTopologyService,
                                                        NodeTopologyService nodeTopologyService) {
        return new IsisOnmsTopologyUpdater(topologyDao, isisTopologyService, nodeTopologyService);
    }

    @Bean
    public OspfOnmsTopologyUpdater ospfTopologyUpdater(OnmsTopologyDao topologyDao,
                                                        OspfTopologyService ospfTopologyService,
                                                        NodeTopologyService nodeTopologyService) {
        return new OspfOnmsTopologyUpdater(topologyDao, ospfTopologyService, nodeTopologyService);
    }

    @Bean
    public OspfAreaOnmsTopologyUpdater ospfAreaTopologyUpdater(OnmsTopologyDao topologyDao,
                                                                OspfTopologyService ospfTopologyService,
                                                                NodeTopologyService nodeTopologyService) {
        return new OspfAreaOnmsTopologyUpdater(topologyDao, ospfTopologyService, nodeTopologyService);
    }

    @Bean
    public BridgeOnmsTopologyUpdater bridgeTopologyUpdater(OnmsTopologyDao topologyDao,
                                                            BridgeTopologyService bridgeTopologyService,
                                                            NodeTopologyService nodeTopologyService) {
        return new BridgeOnmsTopologyUpdater(topologyDao, bridgeTopologyService, nodeTopologyService);
    }

    @Bean
    public NetworkRouterTopologyUpdater networkRouterTopologyUpdater(OnmsTopologyDao topologyDao,
                                                                      NodeTopologyService nodeTopologyService) {
        return new NetworkRouterTopologyUpdater(topologyDao, nodeTopologyService);
    }

    @Bean
    public UserDefinedLinkTopologyUpdater userDefinedLinkTopologyUpdater(
            UserDefinedLinkTopologyService udlTopologyService,
            OnmsTopologyDao topologyDao,
            NodeTopologyService nodeTopologyService) {
        return new UserDefinedLinkTopologyUpdater(udlTopologyService, topologyDao, nodeTopologyService);
    }

    @Bean
    public DiscoveryBridgeDomains discoveryBridgeDomains(BridgeTopologyService bridgeTopologyService) {
        return new DiscoveryBridgeDomains(bridgeTopologyService);
    }

    // ── 8. TopologyUpdaterRegistry + EnhancedLinkd daemon ──────────

    @Bean
    public TopologyUpdaterRegistry topologyUpdaterRegistry(
            NodesOnmsTopologyUpdater nodesTopologyUpdater,
            CdpOnmsTopologyUpdater cdpTopologyUpdater,
            LldpOnmsTopologyUpdater lldpTopologyUpdater,
            IsisOnmsTopologyUpdater isisTopologyUpdater,
            OspfOnmsTopologyUpdater ospfTopologyUpdater,
            OspfAreaOnmsTopologyUpdater ospfAreaTopologyUpdater,
            BridgeOnmsTopologyUpdater bridgeTopologyUpdater,
            NetworkRouterTopologyUpdater networkRouterTopologyUpdater,
            UserDefinedLinkTopologyUpdater userDefinedLinkTopologyUpdater,
            DiscoveryBridgeDomains discoveryBridgeDomains) {
        var registry = new TopologyUpdaterRegistry();
        registry.put(ProtocolSupported.NODES, nodesTopologyUpdater);
        registry.put(ProtocolSupported.CDP, cdpTopologyUpdater);
        registry.put(ProtocolSupported.LLDP, lldpTopologyUpdater);
        registry.put(ProtocolSupported.ISIS, isisTopologyUpdater);
        registry.put(ProtocolSupported.OSPF, ospfTopologyUpdater);
        registry.put(ProtocolSupported.OSPFAREA, ospfAreaTopologyUpdater);
        registry.put(ProtocolSupported.BRIDGE, bridgeTopologyUpdater);
        registry.put(ProtocolSupported.NETWORKROUTER, networkRouterTopologyUpdater);
        registry.put(ProtocolSupported.USERDEFINED, userDefinedLinkTopologyUpdater);
        registry.setDiscoveryBridgeDomains(discoveryBridgeDomains);
        return registry;
    }

    @Bean
    public EnhancedLinkd enhancedLinkd(EnhancedLinkdConfig linkdConfig,
                                        NodeTopologyService nodeTopologyService,
                                        CdpTopologyService cdpTopologyService,
                                        LldpTopologyService lldpTopologyService,
                                        IsisTopologyService isisTopologyService,
                                        OspfTopologyService ospfTopologyService,
                                        BridgeTopologyService bridgeTopologyService,
                                        IpNetToMediaTopologyService ipNetToMediaTopologyService,
                                        LocationAwareSnmpClient locationAwareSnmpClient,
                                        TopologyUpdaterRegistry registry) {
        return new EnhancedLinkd(
                linkdConfig, nodeTopologyService,
                bridgeTopologyService, cdpTopologyService,
                isisTopologyService, ipNetToMediaTopologyService,
                lldpTopologyService, ospfTopologyService,
                locationAwareSnmpClient,
                registry);
    }

    // ── 9. Event Processor ───────────────────────────────────────────

    @Bean
    public EventProcessor eventProcessor(EnhancedLinkd linkd) {
        var processor = new EventProcessor();
        processor.setLinkd(linkd);
        // Do NOT call init() — that subscribes to MessageBus which is handled
        // separately via the AnnotationBasedEventListenerAdapter
        return processor;
    }

    @Bean
    public AnnotationBasedEventListenerAdapter enlinkdEventListener(
            EventProcessor eventProcessor,
            EventSubscriptionService eventSubscriptionService) {
        // Use no-arg constructor + setters so afterPropertiesSet() is called
        // exactly once by Spring's InitializingBean contract. The two-arg
        // constructor also calls afterPropertiesSet(), which would double-register.
        var adapter = new AnnotationBasedEventListenerAdapter();
        adapter.setAnnotatedListener(eventProcessor);
        adapter.setEventSubscriptionService(eventSubscriptionService);
        return adapter;
    }

    // ── 10. SmartLifecycle ───────────────────────────────────────────

    @Bean
    public SmartLifecycle enlinkdLifecycle(EnhancedLinkd daemon) {
        return new SpringServiceDaemonSmartLifecycle(daemon);
    }

}
