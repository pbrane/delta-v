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

import org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.enlinkd.model.BridgeBridgeLink;
import org.opennms.netmgt.enlinkd.model.BridgeElement;
import org.opennms.netmgt.enlinkd.model.BridgeMacLink;
import org.opennms.netmgt.enlinkd.model.BridgeStpLink;
import org.opennms.netmgt.enlinkd.model.CdpElement;
import org.opennms.netmgt.enlinkd.model.CdpLink;
import org.opennms.netmgt.enlinkd.model.IpNetToMedia;
import org.opennms.netmgt.enlinkd.model.IsIsElement;
import org.opennms.netmgt.enlinkd.model.IsIsLink;
import org.opennms.netmgt.enlinkd.model.LldpElement;
import org.opennms.netmgt.enlinkd.model.LldpLink;
import org.opennms.netmgt.enlinkd.model.OspfArea;
import org.opennms.netmgt.enlinkd.model.OspfElement;
import org.opennms.netmgt.enlinkd.model.OspfLink;
import org.opennms.netmgt.enlinkd.model.UserDefinedLink;
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
import org.opennms.netmgt.enlinkd.persistence.impl.TopologyEntityDaoJpa;
import org.opennms.netmgt.enlinkd.persistence.impl.UserDefinedLinkDaoJpa;
import org.opennms.netmgt.model.OnmsApplication;
import org.opennms.netmgt.model.OnmsCategory;
import org.opennms.netmgt.model.OnmsDistPoller;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.OnmsMonitoringSystem;
import org.opennms.netmgt.model.OnmsAssetRecord;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsServiceType;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opennms.netmgt.model.jakarta.converter.InetAddressConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeLabelSourceConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeTypeConverter;
import org.opennms.netmgt.model.jakarta.converter.OnmsSeverityConverter;
import org.opennms.netmgt.model.jakarta.converter.PrimaryTypeConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring Boot configuration for Enlinkd JPA entities, DAOs, and infrastructure beans.
 *
 * <p>Enlinkd requires the core node/interface entities plus all 15 link discovery
 * entities (CDP, LLDP, OSPF, IS-IS, Bridge, IpNetToMedia, UserDefinedLink).
 * Entity classes are listed explicitly via {@link PersistenceManagedTypes} to avoid
 * scanning legacy javax.persistence classes that are incompatible with Hibernate 7.</p>
 */
@Configuration
@EnableTransactionManagement
public class EnlinkdJpaConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(EnlinkdJpaConfiguration.class);

    // ===================================================================
    // Section 1: JPA / Naming
    // ===================================================================

    @Bean
    public PhysicalNamingStrategyStandardImpl physicalNamingStrategy() {
        return new PhysicalNamingStrategyStandardImpl();
    }

    /**
     * Explicitly lists all Jakarta entity classes needed by Enlinkd:
     * core entities (Node, IpInterface, SnmpInterface, MonitoringLocation,
     * DistPoller, MonitoringSystem, Category) plus all 15 Enlinkd entities.
     *
     * <p>Converter classes are listed explicitly so Hibernate 7 registers
     * {@code @Converter(autoApply=true)} converters (NodeType, PrimaryType,
     * InetAddress, etc.) that map enum/custom types to DB column values.</p>
     */
    @Bean
    public PersistenceManagedTypes persistenceManagedTypes() {
        return PersistenceManagedTypes.of(
            // Core entities
            OnmsNode.class.getName(),
            OnmsAssetRecord.class.getName(),
            OnmsIpInterface.class.getName(),
            OnmsSnmpInterface.class.getName(),
            OnmsMonitoringLocation.class.getName(),
            OnmsDistPoller.class.getName(),
            OnmsMonitoringSystem.class.getName(),
            OnmsCategory.class.getName(),
            OnmsMonitoredService.class.getName(),
            OnmsServiceType.class.getName(),
            OnmsApplication.class.getName(),
            // Enlinkd entities (Jakarta)
            CdpLink.class.getName(),
            CdpElement.class.getName(),
            LldpLink.class.getName(),
            LldpElement.class.getName(),
            OspfLink.class.getName(),
            OspfElement.class.getName(),
            OspfArea.class.getName(),
            IsIsLink.class.getName(),
            IsIsElement.class.getName(),
            IpNetToMedia.class.getName(),
            BridgeBridgeLink.class.getName(),
            BridgeMacLink.class.getName(),
            BridgeStpLink.class.getName(),
            BridgeElement.class.getName(),
            UserDefinedLink.class.getName(),
            // AttributeConverters (autoApply=true)
            NodeTypeConverter.class.getName(),
            PrimaryTypeConverter.class.getName(),
            InetAddressConverter.class.getName(),
            NodeLabelSourceConverter.class.getName(),
            OnmsSeverityConverter.class.getName()
        );
    }

    // ===================================================================
    // Section 2: Transaction / SessionUtils
    // ===================================================================

    @Bean
    public SessionUtils sessionUtils(PlatformTransactionManager txManager) {
        var txTemplate = new TransactionTemplate(txManager);
        var readOnlyTxTemplate = new TransactionTemplate(txManager);
        readOnlyTxTemplate.setReadOnly(true);
        return new SessionUtils() {
            @Override
            public <V> V withTransaction(java.util.function.Supplier<V> supplier) {
                return txTemplate.execute(status -> supplier.get());
            }
            @Override
            public <V> V withReadOnlyTransaction(java.util.function.Supplier<V> supplier) {
                return readOnlyTxTemplate.execute(status -> supplier.get());
            }
            @Override
            public <V> V withManualFlush(java.util.function.Supplier<V> supplier) {
                return supplier.get();
            }
        };
    }

    @Bean
    public TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }

    // ===================================================================
    // Section 3: Enlinkd JPA DAOs (15 beans)
    // ===================================================================

    @Bean
    public CdpLinkDaoJpa cdpLinkDaoJpa() {
        return new CdpLinkDaoJpa();
    }

    @Bean
    public CdpElementDaoJpa cdpElementDaoJpa() {
        return new CdpElementDaoJpa();
    }

    @Bean
    public LldpLinkDaoJpa lldpLinkDaoJpa() {
        return new LldpLinkDaoJpa();
    }

    @Bean
    public LldpElementDaoJpa lldpElementDaoJpa() {
        return new LldpElementDaoJpa();
    }

    @Bean
    public OspfLinkDaoJpa ospfLinkDaoJpa() {
        return new OspfLinkDaoJpa();
    }

    @Bean
    public OspfElementDaoJpa ospfElementDaoJpa() {
        return new OspfElementDaoJpa();
    }

    @Bean
    public OspfAreaDaoJpa ospfAreaDaoJpa() {
        return new OspfAreaDaoJpa();
    }

    @Bean
    public IsIsLinkDaoJpa isisLinkDaoJpa() {
        return new IsIsLinkDaoJpa();
    }

    @Bean
    public IsIsElementDaoJpa isisElementDaoJpa() {
        return new IsIsElementDaoJpa();
    }

    @Bean
    public IpNetToMediaDaoJpa ipNetToMediaDaoJpa() {
        return new IpNetToMediaDaoJpa();
    }

    @Bean
    public BridgeElementDaoJpa bridgeElementDaoJpa() {
        return new BridgeElementDaoJpa();
    }

    @Bean
    public BridgeBridgeLinkDaoJpa bridgeBridgeLinkDaoJpa() {
        return new BridgeBridgeLinkDaoJpa();
    }

    @Bean
    public BridgeMacLinkDaoJpa bridgeMacLinkDaoJpa() {
        return new BridgeMacLinkDaoJpa();
    }

    @Bean
    public BridgeStpLinkDaoJpa bridgeStpLinkDaoJpa() {
        return new BridgeStpLinkDaoJpa();
    }

    @Bean
    public UserDefinedLinkDaoJpa userDefinedLinkDaoJpa() {
        return new UserDefinedLinkDaoJpa();
    }

    @Bean
    public TopologyEntityDaoJpa topologyEntityDaoJpa() {
        return new TopologyEntityDaoJpa();
    }

}
