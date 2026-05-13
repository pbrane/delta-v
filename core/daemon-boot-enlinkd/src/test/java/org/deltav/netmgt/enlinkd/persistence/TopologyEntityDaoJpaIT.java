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
package org.deltav.netmgt.enlinkd.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.deltav.core.daemon.common.EventConfEnrichmentService;
import org.deltav.core.event.forwarder.kafka.KafkaEventForwarder;
import org.deltav.core.event.forwarder.kafka.KafkaEventSubscriptionService;
import org.opennms.core.rpc.api.RpcClientFactory;
import org.opennms.core.utils.LldpUtils.LldpChassisIdSubType;
import org.opennms.core.utils.LldpUtils.LldpPortIdSubType;
import org.opennms.netmgt.config.EnhancedLinkdConfig;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.enlinkd.EnhancedLinkd;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.opennms.netmgt.enlinkd.model.CdpElement;
import org.opennms.netmgt.enlinkd.model.CdpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.CdpLink;
import org.opennms.netmgt.enlinkd.model.CdpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsElement;
import org.opennms.netmgt.enlinkd.model.IsIsElement.IsisAdminState;
import org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.IsIsLink;
import org.opennms.netmgt.enlinkd.model.IsIsLink.IsisISAdjNeighSysType;
import org.opennms.netmgt.enlinkd.model.IsIsLink.IsisISAdjState;
import org.opennms.netmgt.enlinkd.model.LldpElement;
import org.opennms.netmgt.enlinkd.model.LldpElementTopologyEntity;
import org.opennms.netmgt.enlinkd.model.LldpLink;
import org.opennms.netmgt.enlinkd.model.LldpLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.NodeTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfArea;
import org.opennms.netmgt.enlinkd.model.OspfAreaTopologyEntity;
import org.opennms.netmgt.enlinkd.model.OspfElement;
import org.opennms.netmgt.enlinkd.model.OspfElement.TruthValue;
import org.opennms.netmgt.enlinkd.model.CdpLink.CiscoNetworkProtocolType;
import org.opennms.netmgt.enlinkd.model.CdpElement.CdpGlobalDeviceIdFormat;
import org.opennms.netmgt.enlinkd.model.OspfLink;
import org.opennms.netmgt.enlinkd.model.OspfLinkTopologyEntity;
import org.opennms.netmgt.enlinkd.model.SnmpInterfaceTopologyEntity;
import org.opennms.netmgt.enlinkd.persistence.api.TopologyEntityDao;
import org.opennms.netmgt.enlinkd.persistence.impl.TopologyEntityDaoJpa;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.PrimaryType;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

/**
 * Integration test for {@link TopologyEntityDaoJpa}. Boots a Postgres
 * Testcontainer with Hibernate-generated schema, inserts one fixture row
 * per source table, and verifies all 11 JPQL constructor projections.
 *
 * <p>JPQL constructor projection ({@code select new FQCN(...)}) is
 * resolved reflectively at query-execution time, so a field-order swap
 * compiles cleanly and fails at runtime. This IT catches that class of bug.</p>
 *
 * <p>All 11 tests currently FAIL with "expected size 1 but was 0" because
 * {@link TopologyEntityDaoJpa} returns empty lists — driving Task 4's
 * query implementation.</p>
 */
@SpringBootTest(classes = org.deltav.netmgt.enlinkd.boot.EnlinkdBootApplication.class,
                webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
@Testcontainers
@Transactional
@Import(TopologyEntityDaoJpaIT.TestConfig.class)
class TopologyEntityDaoJpaIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("opennms")
            .withUsername("opennms")
            .withPassword("opennms");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("opennms.kafka.bootstrap-servers", () -> "localhost:9092");
    }

    /**
     * Registers the {@link TopologyEntityDaoJpa} bean being tested — it is
     * not in the component-scan packages of {@code EnlinkdBootApplication} and
     * will be wired in Task 5 (EnlinkdDaemonConfiguration integration).
     */
    @TestConfiguration
    static class TestConfig {
        @Bean
        public TopologyEntityDao topologyEntityDao() {
            return new TopologyEntityDaoJpa();
        }
    }

    /**
     * Replaces the file-loading snmpPeerFactory from EnlinkdDaemonConfiguration
     * with a mock. @MockitoBean wins over any production bean of the same type.
     */
    @MockitoBean
    private SnmpAgentConfigFactory snmpAgentConfigFactory;

    /** Replaces the Kafka RPC client factory with a mock. */
    @MockitoBean
    private RpcClientFactory rpcClientFactory;

    /**
     * Replaces the Kafka-backed EventIpcManager with a mock.
     *
     * <p>EventIpcManager extends EventSubscriptionService so a single mock of
     * the broader type satisfies injection points for both interfaces.
     * Named explicitly to prevent type-based lookup from colliding with the
     * KafkaEventSubscriptionService bean which also implements EventSubscriptionService.</p>
     */
    @MockitoBean(name = "eventIpcManager")
    private EventIpcManager eventIpcManager;

    /**
     * Replaces the KafkaEventForwarder bean with a mock of the concrete class.
     *
     * <p>The factory method KafkaEventForwarderFactory.create() attempts to create
     * a Kafka producer which connects to the bootstrap server. Mocking the concrete
     * class prevents that connection attempt. The bean name matches the production
     * bean name "kafkaEventForwarder".</p>
     */
    @MockitoBean(name = "kafkaEventForwarder")
    private KafkaEventForwarder kafkaEventForwarder;

    /**
     * Replaces the KafkaEventSubscriptionService with a mock of the concrete class.
     *
     * <p>kafkaEventSubscriptionLifecycle takes KafkaEventSubscriptionService by its
     * concrete type, so the mock must also be of that type (not just the interface).
     * This prevents the Kafka consumer from being created and connecting to bootstrap.</p>
     */
    @MockitoBean(name = "kafkaEventSubscriptionService")
    private KafkaEventSubscriptionService kafkaEventSubscriptionService;

    /**
     * Replaces the file-loading EnhancedLinkdConfigFactory bean.
     *
     * <p>The production {@code linkdConfig()} bean factory reads
     * {@code /opt/deltav/etc/enlinkd-config.xml}. The mock prevents
     * the factory method from running so the file is never read.</p>
     */
    @MockitoBean
    private EnhancedLinkdConfig enhancedLinkdConfig;

    /**
     * Replaces the entire EnhancedLinkd daemon with a mock.
     *
     * <p>EnhancedLinkd implements InitializingBean (via AbstractServiceDaemon).
     * Spring calls afterPropertiesSet() immediately on bean creation; onInit()
     * constructs a LegacyScheduler with thread-pool size taken from
     * EnhancedLinkdConfig. With a mocked config, all int getters return 0 and
     * ThreadPoolExecutor rejects nThreads=0 with IllegalArgumentException.
     * Mocking the daemon itself prevents the @Bean factory method from running
     * and bypasses the InitializingBean lifecycle entirely.</p>
     */
    @MockitoBean
    private EnhancedLinkd enhancedLinkd;

    /**
     * Replaces the EventConfEnrichmentService to prevent a startup DB query.
     *
     * <p>EventConfEnrichmentService is @ConditionalOnProperty("spring.datasource.url"),
     * which is always true when Testcontainers provides a JDBC URL. Its constructor
     * queries the eventconf_sources table which does not exist in the Hibernate
     * create-drop schema (it is a delta-v native table, not an entity). The mock
     * satisfies any downstream beans that inject this service.</p>
     */
    @MockitoBean
    private EventConfEnrichmentService eventConfEnrichmentService;

    @Autowired
    private TopologyEntityDao dao;

    @PersistenceContext
    private EntityManager em;

    @BeforeEach
    void insertFixtures() throws Exception {
        // Monitoring location — required FK for OnmsNode
        OnmsMonitoringLocation location = new OnmsMonitoringLocation("Default", "Default");
        em.persist(location);

        // Node — base entity referenced by all enlinkd entities
        OnmsNode node = new OnmsNode(location, "fixture-node");
        node.setType(OnmsNode.NodeType.ACTIVE);
        node.setSysObjectId(".1.3.6.1.4.1.9");
        em.persist(node);

        // SNMP Interface — needed by SnmpInterfaceTopologyEntity and IpInterfaceTopologyEntity
        OnmsSnmpInterface snmp = new OnmsSnmpInterface(node, 1);
        snmp.setIfName("eth0");
        snmp.setIfAlias("uplink");
        snmp.setIfSpeed(1_000_000_000L);
        em.persist(snmp);

        // IP Interface — needed by IpInterfaceTopologyEntity
        OnmsIpInterface ip = new OnmsIpInterface(InetAddress.getByName("192.0.2.1"), node);
        ip.setNetMask(InetAddress.getByName("255.255.255.0"));
        ip.setIsManaged("M");
        ip.setIsSnmpPrimary(PrimaryType.PRIMARY);
        ip.setSnmpInterface(snmp);
        em.persist(ip);

        // LldpLink — lldpPortIdSubType, lldpRemChassisIdSubType, lldpRemPortIdSubType
        //            are NOT NULL enums; lldpRemLocalPortNum and lldpRemIndex are NOT NULL
        LldpLink lldpLink = new LldpLink();
        lldpLink.setNode(node);
        lldpLink.setLldpRemLocalPortNum(1);
        lldpLink.setLldpRemIndex(1);
        lldpLink.setLldpPortIdSubType(LldpPortIdSubType.LLDP_PORTID_SUBTYPE_INTERFACENAME);
        lldpLink.setLldpPortId("Gi0/2");
        lldpLink.setLldpPortDescr("local-iface");
        lldpLink.setLldpPortIfindex(1);
        lldpLink.setLldpRemChassisId("aa:bb:cc:dd:ee:ff");
        lldpLink.setLldpRemChassisIdSubType(LldpChassisIdSubType.LLDP_CHASSISID_SUBTYPE_MACADDRESS);
        lldpLink.setLldpRemPortIdSubType(LldpPortIdSubType.LLDP_PORTID_SUBTYPE_INTERFACENAME);
        lldpLink.setLldpRemPortId("Gi0/1");
        lldpLink.setLldpRemPortDescr("peer-iface");
        lldpLink.setLldpRemSysname("peer-1");
        lldpLink.setLldpLinkCreateTime(new Date());
        lldpLink.setLldpLinkLastPollTime(new Date());
        em.persist(lldpLink);

        // LldpElement — lldpChassisIdSubType is NOT NULL
        LldpElement lldpElement = new LldpElement();
        lldpElement.setNode(node);
        lldpElement.setLldpChassisId("11:22:33:44:55:66");
        lldpElement.setLldpChassisIdSubType(LldpChassisIdSubType.LLDP_CHASSISID_SUBTYPE_MACADDRESS);
        lldpElement.setLldpSysname("local-host");
        lldpElement.setLldpNodeCreateTime(new Date());
        lldpElement.setLldpNodeLastPollTime(new Date());
        em.persist(lldpElement);

        // CdpLink — cdpCacheDeviceIndex, cdpCacheAddressType, cdpCacheVersion,
        //           cdpCacheDeviceId, cdpCacheDevicePort, cdpCacheDevicePlatform NOT NULL
        CdpLink cdpLink = new CdpLink();
        cdpLink.setNode(node);
        cdpLink.setCdpCacheIfIndex(1);
        cdpLink.setCdpCacheDeviceIndex(1);
        cdpLink.setCdpCacheAddressType(CiscoNetworkProtocolType.ip);
        cdpLink.setCdpInterfaceName("Gi0/1");
        cdpLink.setCdpCacheAddress("192.0.2.2");
        cdpLink.setCdpCacheVersion("Cisco IOS 15.1");
        cdpLink.setCdpCacheDeviceId("cdp-peer");
        cdpLink.setCdpCacheDevicePort("Gi0/0");
        cdpLink.setCdpCacheDevicePlatform("Cisco 3750");
        cdpLink.setCdpLinkCreateTime(new Date());
        cdpLink.setCdpLinkLastPollTime(new Date());
        em.persist(cdpLink);

        // CdpElement — cdpGlobalRun NOT NULL
        CdpElement cdpElement = new CdpElement();
        cdpElement.setNode(node);
        cdpElement.setCdpGlobalRun(TruthValue.TRUE);
        cdpElement.setCdpGlobalDeviceId("global-dev-id");
        cdpElement.setCdpGlobalDeviceIdFormat(CdpGlobalDeviceIdFormat.serialNumber);
        cdpElement.setCdpNodeCreateTime(new Date());
        cdpElement.setCdpNodeLastPollTime(new Date());
        em.persist(cdpElement);

        // OspfLink — ospfRemRouterId NOT NULL; ospfIfAreaId is InetAddress (not int)
        OspfLink ospfLink = new OspfLink();
        ospfLink.setNode(node);
        ospfLink.setOspfIpAddr(InetAddress.getByName("10.0.0.1"));
        ospfLink.setOspfIpMask(InetAddress.getByName("255.255.255.0"));
        ospfLink.setOspfRemRouterId(InetAddress.getByName("10.0.0.2"));
        ospfLink.setOspfRemIpAddr(InetAddress.getByName("10.0.0.2"));
        ospfLink.setOspfRemAddressLessIndex(0);
        ospfLink.setOspfIfIndex(1);
        ospfLink.setOspfIfAreaId(InetAddress.getByName("0.0.0.0"));
        ospfLink.setOspfLinkCreateTime(new Date());
        ospfLink.setOspfLinkLastPollTime(new Date());
        em.persist(ospfLink);

        // OspfArea — ospfAreaId is InetAddress (not int)
        OspfArea ospfArea = new OspfArea();
        ospfArea.setNode(node);
        ospfArea.setOspfAreaId(InetAddress.getByName("0.0.0.0"));
        ospfArea.setOspfAuthType(0);
        ospfArea.setOspfImportAsExtern(1);
        ospfArea.setOspfAreaBdrRtrCount(1);
        ospfArea.setOspfAsBdrRtrCount(0);
        ospfArea.setOspfAreaLsaCount(10);
        ospfArea.setOspfAreaLastPollTime(new Date());
        em.persist(ospfArea);

        // IsIsLink — isisCircIndex, isisISAdjIndex, isisISAdjState,
        //            isisISAdjNeighSNPAAddress, isisISAdjNeighSysType,
        //            isisISAdjNbrExtendedCircID NOT NULL
        IsIsLink isisLink = new IsIsLink();
        isisLink.setNode(node);
        isisLink.setIsisCircIndex(1);
        isisLink.setIsisISAdjIndex(1);
        isisLink.setIsisCircIfIndex(1);
        isisLink.setIsisISAdjState(IsisISAdjState.up);
        isisLink.setIsisISAdjNeighSysID("isis-peer");
        isisLink.setIsisISAdjNeighSNPAAddress("aa:bb:cc:dd:ee:ff");
        isisLink.setIsisISAdjNeighSysType(IsisISAdjNeighSysType.l1L2IntermediateSystem);
        isisLink.setIsisISAdjNbrExtendedCircID(0);
        isisLink.setIsisLinkCreateTime(new Date());
        isisLink.setIsisLinkLastPollTime(new Date());
        em.persist(isisLink);

        // IsIsElement — isisSysAdminState NOT NULL
        IsIsElement isisElement = new IsIsElement();
        isisElement.setNode(node);
        isisElement.setIsisSysID("local-sys");
        isisElement.setIsisSysAdminState(IsisAdminState.on);
        isisElement.setIsisNodeCreateTime(new Date());
        isisElement.setIsisNodeLastPollTime(new Date());
        em.persist(isisElement);

        em.flush();
    }

    @Test
    void nodeProjectionPopulates() {
        List<NodeTopologyEntity> result = dao.getNodeTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLabel()).isEqualTo("fixture-node");
    }

    @Test
    void cdpLinkProjectionPopulates() {
        List<CdpLinkTopologyEntity> result = dao.getCdpLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCdpCacheDeviceId()).isEqualTo("cdp-peer");
    }

    @Test
    void cdpElementProjectionPopulates() {
        List<CdpElementTopologyEntity> result = dao.getCdpElementTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getCdpGlobalDeviceId()).isEqualTo("global-dev-id");
    }

    @Test
    void lldpLinkProjectionPopulates() {
        List<LldpLinkTopologyEntity> result = dao.getLldpLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLldpRemSysname()).isEqualTo("peer-1");
    }

    @Test
    void lldpElementProjectionPopulates() {
        List<LldpElementTopologyEntity> result = dao.getLldpElementTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLldpSysname()).isEqualTo("local-host");
    }

    @Test
    void ospfLinkProjectionPopulates() {
        List<OspfLinkTopologyEntity> result = dao.getOspfLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOspfIpAddr().getHostAddress()).isEqualTo("10.0.0.1");
    }

    @Test
    void ospfAreaProjectionPopulates() {
        List<OspfAreaTopologyEntity> result = dao.getOspfAreaTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOspfAreaId().getHostAddress()).isEqualTo("0.0.0.0");
    }

    @Test
    void isisLinkProjectionPopulates() {
        List<org.opennms.netmgt.enlinkd.model.IsIsLinkTopologyEntity> result = dao.getIsIsLinkTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIsisISAdjNeighSysID()).isEqualTo("isis-peer");
    }

    @Test
    void isisElementProjectionPopulates() {
        List<org.opennms.netmgt.enlinkd.model.IsIsElementTopologyEntity> result = dao.getIsIsElementTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIsisSysID()).isEqualTo("local-sys");
    }

    @Test
    void snmpInterfaceProjectionPopulates() {
        List<SnmpInterfaceTopologyEntity> result = dao.getSnmpTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIfName()).isEqualTo("eth0");
    }

    @Test
    void ipInterfaceProjectionPopulates() {
        List<org.opennms.netmgt.enlinkd.model.IpInterfaceTopologyEntity> result = dao.getIpTopologyEntities();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getIpAddress().getHostAddress()).isEqualTo("192.0.2.1");
    }
}
