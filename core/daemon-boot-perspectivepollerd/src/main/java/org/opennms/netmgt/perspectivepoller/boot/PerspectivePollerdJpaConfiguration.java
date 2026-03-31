/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.netmgt.perspectivepoller.boot;

import java.io.IOException;
import java.io.UncheckedIOException;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl;
import org.opennms.netmgt.collection.api.AttributeGroup;
import org.opennms.netmgt.collection.api.CollectionAgentFactory;
import org.opennms.netmgt.collection.api.CollectionAttribute;
import org.opennms.netmgt.collection.api.CollectionResource;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.Persister;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collection.api.ServiceParameters;
import org.opennms.netmgt.config.api.DefaultDatabaseSchemaConfig;
import org.opennms.netmgt.config.filter.DatabaseSchema;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.eventd.EventUtil;
import org.opennms.netmgt.filter.FilterDaoFactory;
import org.opennms.netmgt.filter.JdbcFilterDao;
import org.opennms.netmgt.model.OnmsApplication;
import org.opennms.netmgt.model.OnmsCategory;
import org.opennms.netmgt.model.OnmsDistPoller;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.OnmsMonitoringSystem;
import org.opennms.netmgt.model.OnmsAssetRecord;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsOutage;
import org.opennms.netmgt.model.OnmsServiceType;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.opennms.netmgt.model.jakarta.converter.InetAddressConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeLabelSourceConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeTypeConverter;
import org.opennms.netmgt.model.jakarta.converter.OnmsSeverityConverter;
import org.opennms.netmgt.model.jakarta.converter.PrimaryTypeConverter;
import org.opennms.netmgt.rrd.RrdRepository;
import org.opennms.netmgt.threshd.api.ThresholdInitializationException;
import org.opennms.netmgt.threshd.api.ThresholdingService;
import org.opennms.netmgt.threshd.api.ThresholdingSession;
import org.opennms.netmgt.threshd.api.ThresholdingSetPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring Boot @Configuration for PerspectivePollerd JPA entities, DAOs, and infrastructure beans.
 *
 * <p>Follows the same pattern as PollerdJpaConfiguration: explicit entity listing
 * (not @EntityScan), SessionUtils for transaction management, and no-op stubs
 * for features not needed by standalone PerspectivePollerd.</p>
 *
 * <p>Entity classes are listed explicitly via a custom {@link PersistenceManagedTypes}
 * bean instead of using package-based {@code @EntityScan} because the legacy
 * opennms-model module shares the same package ({@code org.opennms.netmgt.model})
 * and contains classes with incompatible javax.persistence / Hibernate 3.x
 * annotations that cause scanning failures with Hibernate 7.</p>
 */
@Configuration
@EnableTransactionManagement
public class PerspectivePollerdJpaConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(PerspectivePollerdJpaConfiguration.class);

    private static final XmlMapper XML_MAPPER;
    static {
        XML_MAPPER = XmlMapper.builder().defaultUseWrapper(false).build();
        XML_MAPPER.registerModule(new JaxbAnnotationModule());
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    // ===================================================================
    // Section 1: JPA / Naming
    // ===================================================================

    /**
     * Use standard JPA naming -- table/column names from @Table/@Column annotations
     * are used as-is, without Spring Boot's default CamelCase to snake_case conversion.
     * Required because the OpenNMS schema uses camelCase table names
     * (e.g., monitoringSystems, ipInterface, ifServices).
     */
    @Bean
    public PhysicalNamingStrategyStandardImpl physicalNamingStrategy() {
        return new PhysicalNamingStrategyStandardImpl();
    }

    /**
     * Explicitly lists the Jakarta entity classes needed by PerspectivePollerd.
     * This replaces {@code @EntityScan(basePackages = "org.opennms.netmgt.model")}
     * which would scan ALL classes in the package, including legacy entities
     * with incompatible javax.persistence annotations.
     */
    @Bean
    public PersistenceManagedTypes persistenceManagedTypes() {
        return PersistenceManagedTypes.of(
            OnmsNode.class.getName(),
            OnmsAssetRecord.class.getName(),
            OnmsIpInterface.class.getName(),
            OnmsMonitoredService.class.getName(),
            OnmsServiceType.class.getName(),
            OnmsOutage.class.getName(),
            OnmsMonitoringSystem.class.getName(),
            OnmsMonitoringLocation.class.getName(),
            OnmsDistPoller.class.getName(),
            OnmsCategory.class.getName(),
            OnmsSnmpInterface.class.getName(),
            OnmsApplication.class.getName(),
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

    /**
     * SessionUtils implementation backed by Spring's TransactionTemplate.
     * Replaces the OSGi-era SessionUtils that was used to bridge Hibernate
     * sessions across OSGi bundles.
     */
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

    /**
     * TransactionTemplate for daemon components that need explicit transaction control.
     */
    @Bean
    public TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }

    // ===================================================================
    // Section 3: FilterDaoFactory initialization
    // ===================================================================

    /**
     * Initializes FilterDaoFactory with a JDBC-backed FilterDao.
     * Must happen before PollerConfigFactory.init() is called,
     * because filter rule validation requires FilterDaoFactory.getInstance().
     *
     * <p>The FilterDaoFactory is a static singleton. We create a JdbcFilterDao
     * backed by the Spring Boot DataSource and set it on the factory.</p>
     */
    @Bean
    public JdbcFilterDao filterDaoInitializer(DataSource dataSource) {
        LOG.info("Initializing FilterDaoFactory with JdbcFilterDao");
        var jdbcFilterDao = new JdbcFilterDao();
        jdbcFilterDao.setDataSource(dataSource);
        var schemaConfig = loadDatabaseSchemaConfig();
        jdbcFilterDao.setDatabaseSchemaConfigFactory(schemaConfig);
        jdbcFilterDao.afterPropertiesSet();
        FilterDaoFactory.setInstance(jdbcFilterDao);
        return jdbcFilterDao;
    }

    private DefaultDatabaseSchemaConfig loadDatabaseSchemaConfig() {
        try (var is = getClass().getResourceAsStream("/database-schema.xml")) {
            var schema = XML_MAPPER.readValue(is, DatabaseSchema.class);
            return new DefaultDatabaseSchemaConfig(schema);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load database-schema.xml from classpath", e);
        }
    }

    // ===================================================================
    // Section 4: No-op stubs
    // ===================================================================

    /**
     * No-op PersisterFactory -- PerspectivePollerd does not persist collection metrics.
     * Data collection is handled by Collectd; PerspectivePollerd only polls service status
     * from perspective locations.
     */
    @Bean
    public PersisterFactory persisterFactory() {
        return new PersisterFactory() {
            private final Persister noOpPersister = new Persister() {
                @Override public void visitCollectionSet(CollectionSet set) {}
                @Override public void visitResource(CollectionResource resource) {}
                @Override public void visitGroup(AttributeGroup group) {}
                @Override public void visitAttribute(CollectionAttribute attribute) {}
                @Override public void completeAttribute(CollectionAttribute attribute) {}
                @Override public void completeGroup(AttributeGroup group) {}
                @Override public void completeResource(CollectionResource resource) {}
                @Override public void completeCollectionSet(CollectionSet set) {}
                @Override public void persistNumericAttribute(CollectionAttribute attribute) {}
                @Override public void persistStringAttribute(CollectionAttribute attribute) {}
            };

            @Override
            public Persister createPersister(ServiceParameters params, RrdRepository repository) {
                return noOpPersister;
            }

            @Override
            public Persister createPersister(ServiceParameters params, RrdRepository repository,
                    boolean dontPersistCounters, boolean forceStoreByGroup, boolean dontReorderAttributes) {
                return noOpPersister;
            }
        };
    }

    /**
     * No-op ThresholdingService -- PerspectivePollerd does not perform thresholding.
     * Thresholding is applied to collected metrics by Collectd/Telemetryd.
     */
    @Bean
    public ThresholdingService thresholdingService() {
        return new ThresholdingService() {
            @Override
            public ThresholdingSession createSession(int nodeId, String hostAddress,
                    String serviceName, ServiceParameters serviceParameters)
                    throws ThresholdInitializationException {
                return null;
            }

            @Override
            public ThresholdingSetPersister getThresholdingSetPersister() {
                return null;
            }
        };
    }

    /**
     * No-op CollectionAgentFactory -- PerspectivePollerd does not create collection agents
     * locally. It constructs CollectionAgentDTOs directly for perspective response time
     * resource storage.
     */
    @Bean
    public CollectionAgentFactory collectionAgentFactory() {
        return new CollectionAgentFactory() {
            @Override
            public org.opennms.netmgt.collection.api.CollectionAgent createCollectionAgent(
                    OnmsIpInterface ipInterface) {
                throw new UnsupportedOperationException("Not used in standalone PerspectivePollerd");
            }
            @Override
            public org.opennms.netmgt.collection.api.CollectionAgent createCollectionAgent(
                    String nodeCriteria, java.net.InetAddress ipAddr) {
                throw new UnsupportedOperationException("Not used in standalone PerspectivePollerd");
            }
            @Override
            public org.opennms.netmgt.collection.api.CollectionAgent createCollectionAgentAndOverrideLocation(
                    String nodeCriteria, java.net.InetAddress ipAddr, String location) {
                throw new UnsupportedOperationException("Not used in standalone PerspectivePollerd");
            }
        };
    }

    /**
     * Minimal EventUtil implementation for parameter expansion.
     * The full EventUtilDaoImpl depends on the legacy Hibernate DAO layer.
     * This pass-through implementation returns inputs unchanged when no
     * database-backed token resolution is available.
     */
    @Bean
    public EventUtil eventUtil() {
        return new EventUtil() {
            @Override public String expandParms(String inp, org.opennms.netmgt.xml.event.Event event) { return inp; }
            @Override public String expandParms(String inp, org.opennms.netmgt.xml.event.Event event, java.util.Map<String, java.util.Map<String, String>> decode) { return inp; }
            @Override public String getNamedParmValue(String string, org.opennms.netmgt.xml.event.Event event) { return ""; }
            @Override public void expandMapValues(java.util.Map<String, String> parmMap, org.opennms.netmgt.xml.event.Event event) {}
            @Override public String getHardwareFieldValue(String parm, long nodeId) { return ""; }
            @Override public String getHostName(int nodeId, String hostip) { return hostip; }
            @Override public String getEventHost(org.opennms.netmgt.xml.event.Event event) { return ""; }
            @Override public String getIfAlias(long nodeId, String ipAddr) { return ""; }
            @Override public String getAssetFieldValue(String parm, long nodeId) { return ""; }
            @Override public String getForeignId(long nodeId) { return ""; }
            @Override public String getForeignSource(long nodeId) { return ""; }
            @Override public String getNodeLabel(long nodeId) { return ""; }
            @Override public String getNodeLocation(long nodeId) { return ""; }
            @Override public org.opennms.netmgt.eventd.processor.expandable.ExpandableParameterResolver getResolver(String token) { return null; }
            @Override public java.util.Date decodeSnmpV2TcDateAndTime(java.math.BigInteger value) { return new java.util.Date(); }
            @Override public String getPrimaryInterface(long nodeId) { return ""; }
        };
    }
}
