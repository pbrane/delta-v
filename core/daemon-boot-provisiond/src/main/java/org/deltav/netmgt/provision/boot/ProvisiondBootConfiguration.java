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
package org.deltav.netmgt.provision.boot;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.netmgt.config.SnmpAssetAdapterConfig;
import org.opennms.netmgt.config.SnmpAssetAdapterConfigFactory;
import org.opennms.netmgt.config.SnmpPeerFactory;
import org.opennms.netmgt.config.snmp.SnmpConfig;
import org.opennms.netmgt.config.snmpmetadata.SnmpMetadataConfigDao;
import org.opennms.netmgt.provision.SnmpAssetProvisioningAdapter;
import org.opennms.netmgt.provision.SnmpMetadataProvisioningAdapter;

import org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl;
import org.opennms.core.concurrent.PausibleScheduledThreadPoolExecutor;
import org.deltav.core.daemon.common.JdbcDistPollerDao;
import org.deltav.core.daemon.common.JdbcInterfaceToNodeCache;
import org.deltav.core.daemon.common.NoOpEntityScopeProvider;
import org.deltav.core.daemon.registry.DetectorRegistryConfiguration;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.soa.ServiceRegistry;
import org.opennms.core.soa.support.DefaultServiceRegistry;
import org.opennms.core.tasks.DefaultTaskCoordinator;
import org.opennms.netmgt.config.api.DefaultDatabaseSchemaConfig;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.filter.DatabaseSchema;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.dao.api.CategoryDao;
import org.opennms.netmgt.dao.api.IpInterfaceDao;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.MonitoringLocationDao;
import org.opennms.netmgt.dao.api.MonitoringSystemDao;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.dao.api.ProvisiondConfigurationDao;
import org.opennms.netmgt.dao.api.RequisitionedCategoryAssociationDao;
import org.opennms.netmgt.dao.api.ServiceTypeDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.dao.api.SnmpInterfaceDao;
import org.opennms.netmgt.events.api.AnnotationBasedEventListenerAdapter;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.events.api.EventSubscriptionService;
import org.opennms.netmgt.filter.FilterDaoFactory;
import org.opennms.netmgt.filter.JdbcFilterDao;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.model.AlarmAssociation;
import org.opennms.netmgt.model.HwEntityAttributeType;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsCategory;
import org.opennms.netmgt.model.OnmsDistPoller;
import org.opennms.netmgt.model.OnmsHwEntity;
import org.opennms.netmgt.model.OnmsHwEntityAlias;
import org.opennms.netmgt.model.OnmsHwEntityAttribute;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.model.OnmsApplication;
import org.opennms.netmgt.model.OnmsMemo;
import org.opennms.netmgt.model.OnmsMonitoredService;
import org.opennms.netmgt.model.OnmsMonitoringSystem;
import org.opennms.netmgt.model.OnmsAssetRecord;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsReductionKeyMemo;
import org.opennms.netmgt.model.OnmsServiceType;
import org.opennms.netmgt.model.OnmsSnmpInterface;
import org.opennms.netmgt.model.RequisitionedCategoryAssociation;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;
import org.opennms.netmgt.model.jakarta.converter.InetAddressConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeLabelSourceConverter;
import org.opennms.netmgt.model.jakarta.converter.NodeTypeConverter;
import org.opennms.netmgt.model.jakarta.converter.OnmsSeverityConverter;
import org.opennms.netmgt.model.jakarta.converter.PrimaryTypeConverter;
import org.opennms.netmgt.provision.LocationAwareDetectorClient;
import org.opennms.netmgt.provision.LocationAwareDnsLookupClient;
import org.opennms.netmgt.provision.detector.client.rpc.DetectorClientRpcModule;
import org.opennms.netmgt.provision.detector.client.rpc.LocationAwareDetectorClientRpcImpl;
import org.opennms.netmgt.provision.dns.client.rpc.DnsLookupClientRpcModule;
import org.opennms.netmgt.provision.dns.client.rpc.LocationAwareDnsLookupClientRpcImpl;
import org.opennms.netmgt.provision.persist.FilesystemForeignSourceRepository;
import org.opennms.netmgt.provision.persist.ForeignSourceRepository;
import org.opennms.netmgt.provision.persist.FusedForeignSourceRepository;
import org.opennms.netmgt.provision.persist.FasterFilesystemForeignSourceRepository;
import org.opennms.netmgt.provision.persist.JSR223ScriptCache;
import org.opennms.netmgt.provision.service.CoreImportActivities;
import org.opennms.netmgt.provision.service.DefaultPluginRegistry;
import org.opennms.netmgt.provision.service.DefaultProvisionService;
import org.opennms.netmgt.provision.service.ImportJobFactory;
import org.opennms.netmgt.provision.service.ImportScheduler;
import org.opennms.netmgt.provision.service.MonitorHolder;
import org.opennms.netmgt.provision.service.Provisioner;
import org.opennms.netmgt.provision.service.ProvisioningAdapterManager;
import org.opennms.netmgt.provision.service.lifecycle.DefaultLifeCycleRepository;
import org.opennms.netmgt.provision.service.lifecycle.LifeCycle;
import org.opennms.netmgt.provision.service.lifecycle.LifeCycleRepository;
import org.opennms.core.snmp.profile.mapper.impl.SnmpProfileMapperImpl;
import org.opennms.netmgt.snmp.SnmpProfileMapper;
import org.opennms.netmgt.snmp.proxy.LocationAwareSnmpClient;
import org.opennms.netmgt.snmp.proxy.common.LocationAwareSnmpClientRpcImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.FileSystemResource;
import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.scheduling.concurrent.ScheduledExecutorFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring Boot @Configuration that wires all Provisiond beans.
 *
 * <p>Replaces the Karaf-era {@code applicationContext-daemon-loader-provisiond.xml}.
 * All bean definitions are translated from the XML context into explicit @Bean methods.</p>
 */
@Configuration
@Import(DetectorRegistryConfiguration.class)
public class ProvisiondBootConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ProvisiondBootConfiguration.class);

    private static final XmlMapper XML_MAPPER;
    static {
        XML_MAPPER = XmlMapper.builder().defaultUseWrapper(false).build();
        XML_MAPPER.registerModule(new JaxbAnnotationModule());
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Value("${opennms.home:/opt/deltav}")
    private String opennmsHome;

    // ===================================================================
    // Section 1: JPA / Naming
    // ===================================================================

    @Bean
    public PhysicalNamingStrategyStandardImpl physicalNamingStrategy() {
        return new PhysicalNamingStrategyStandardImpl();
    }

    @Bean
    public PersistenceManagedTypes persistenceManagedTypes() {
        return PersistenceManagedTypes.of(
            OnmsAlarm.class.getName(),
            AlarmAssociation.class.getName(),
            OnmsCategory.class.getName(),
            OnmsDistPoller.class.getName(),
            OnmsIpInterface.class.getName(),
            OnmsMemo.class.getName(),
            OnmsMonitoredService.class.getName(),
            OnmsMonitoringSystem.class.getName(),
            OnmsNode.class.getName(),
            OnmsAssetRecord.class.getName(),
            OnmsReductionKeyMemo.class.getName(),
            OnmsServiceType.class.getName(),
            OnmsSnmpInterface.class.getName(),
            OnmsMonitoringLocation.class.getName(),
            OnmsApplication.class.getName(),
            RequisitionedCategoryAssociation.class.getName(),
            // HW inventory entities disabled until HwEntityAttributeType entity registration is fixed:
            // OnmsHwEntity, OnmsHwEntityAttribute, HwEntityAttributeType, OnmsHwEntityAlias
            // AttributeConverters (autoApply=true)
            NodeTypeConverter.class.getName(),
            PrimaryTypeConverter.class.getName(),
            InetAddressConverter.class.getName(),
            NodeLabelSourceConverter.class.getName(),
            OnmsSeverityConverter.class.getName()
        );
    }

    // ===================================================================
    // Section 2: DAO / Cache / Transaction
    // ===================================================================

    // DistPollerDao is provided by DistPollerDaoJpa (component-scanned from jakarta.dao package).
    // Do NOT define an explicit bean here — it conflicts with the @Repository bean.

    @Bean
    public InterfaceToNodeCache interfaceToNodeCache(DataSource dataSource) {
        return new JdbcInterfaceToNodeCache(dataSource);
    }

    @Bean
    public TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }

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

    // ===================================================================
    // Section 3: SNMP Config
    // ===================================================================

    @Bean
    public SnmpAgentConfigFactory snmpPeerFactory(EntityScopeProvider entityScopeProvider) throws IOException {
        var configFile = new File(opennmsHome, "etc/snmp-config.xml");
        LOG.info("Loading SnmpPeerFactory from {}", configFile);
        var config = XML_MAPPER.readValue(configFile, SnmpConfig.class);
        var factory = new SnmpPeerFactory(config, entityScopeProvider, null);
        SnmpPeerFactory.setInstance(factory);
        return factory;
    }

    /**
     * Initializes FilterDaoFactory with a JDBC-backed FilterDao.
     * Required by SnmpProfileMapperImpl (filter-expression evaluation in <snmp-profile>
     * elements) and by horizon code paths that still consult FilterDaoFactory.getInstance().
     *
     * <p>Pattern matches CollectdJpaConfiguration.filterDaoInitializer.
     * The bean name "filterDaoInitializer" is intentional so consumers can use
     * @DependsOn("filterDaoInitializer") to guarantee the static-singleton side
     * effect ran before they resolve.</p>
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
            if (is == null) {
                throw new IllegalStateException(
                        "database-schema.xml not found on classpath — expected from opennms-config jar");
            }
            var schema = XML_MAPPER.readValue(is, DatabaseSchema.class);
            return new DefaultDatabaseSchemaConfig(schema);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load database-schema.xml from classpath", e);
        }
    }

    @Bean
    @DependsOn("filterDaoInitializer")
    public SnmpProfileMapper snmpProfileMapper(
            FilterDao filterDao,
            SnmpAgentConfigFactory snmpAgentConfigFactory,
            LocationAwareSnmpClient locationAwareSnmpClient) {
        return new SnmpProfileMapperImpl(filterDao, snmpAgentConfigFactory, locationAwareSnmpClient);
    }

    // SNMP detector factories are registered via @Import(DetectorRegistryConfiguration.class)
    // at the class level. DetectorRegistryConfiguration pulls the SnmpAgentConfigFactory
    // bean defined above (snmpPeerFactory) via constructor injection.

    // ===================================================================
    // Section 4: RPC Clients
    // ===================================================================

    @Bean
    public LocationAwareSnmpClient locationAwareSnmpClient() {
        return new LocationAwareSnmpClientRpcImpl();
    }

    @Bean
    public DetectorClientRpcModule detectorClientRpcModule() {
        return new DetectorClientRpcModule();
    }

    @Bean
    public LocationAwareDetectorClient locationAwareDetectorClient() {
        return new LocationAwareDetectorClientRpcImpl();
    }

    @Bean
    public DnsLookupClientRpcModule dnsLookupClientRpcModule() {
        return new DnsLookupClientRpcModule(4);
    }

    @Bean
    public LocationAwareDnsLookupClient locationAwareDnsLookupClient() {
        return new LocationAwareDnsLookupClientRpcImpl();
    }

    // ===================================================================
    // Section 5: Event Forwarder
    // ===================================================================

    /**
     * Wraps the upstream {@link EventForwarder} with {@link CountingProvisionEventForwarder}
     * (Micrometer counters per provisiond lifecycle UEI) and then with
     * {@link QualifiedEventForwarder} (the {@code @Qualifier("transactionAware")}
     * marker bean). The chain order is intentional: counting must see every
     * outbound event, including those that {@code QualifiedEventForwarder}
     * would otherwise pass through unchanged today but might mediate later.
     */
    @Bean
    @Qualifier("transactionAware")
    public EventForwarder transactionAwareEventForwarder(EventForwarder eventForwarder,
                                                         MeterRegistry meterRegistry) {
        return new QualifiedEventForwarder(
                new CountingProvisionEventForwarder(eventForwarder, meterRegistry));
    }

    // ===================================================================
    // Section 6: ServiceRegistry
    // ===================================================================

    @Bean
    public ServiceRegistry serviceRegistry() {
        return new DefaultServiceRegistry();
    }

    // ===================================================================
    // Section 7: Provisiond Config
    // ===================================================================

    @Bean
    public ProvisiondConfigurationDao provisiondConfigDao() {
        return new InlineProvisiondConfigDao();
    }

    // ===================================================================
    // Section 8: ForeignSourceRepository (6 instances)
    // ===================================================================

    @Bean
    @Qualifier("filePending")
    public ForeignSourceRepository pendingForeignSourceRepository() {
        var repo = new FilesystemForeignSourceRepository();
        repo.setRequisitionPath(opennmsHome + "/etc/imports/pending");
        repo.setForeignSourcePath(opennmsHome + "/etc/foreign-sources/pending");
        return repo;
    }

    @Bean
    @Qualifier("fileDeployed")
    public ForeignSourceRepository deployedForeignSourceRepository() {
        var repo = new FilesystemForeignSourceRepository();
        repo.setRequisitionPath(opennmsHome + "/etc/imports");
        repo.setForeignSourcePath(opennmsHome + "/etc/foreign-sources");
        return repo;
    }

    @Bean
    @Qualifier("fastFilePending")
    public ForeignSourceRepository fastPendingForeignSourceRepository() {
        var repo = new FasterFilesystemForeignSourceRepository();
        repo.setRequisitionPath(opennmsHome + "/etc/imports/pending");
        repo.setForeignSourcePath(opennmsHome + "/etc/foreign-sources/pending");
        return repo;
    }

    @Bean
    @Qualifier("fastFileDeployed")
    public ForeignSourceRepository fastDeployedForeignSourceRepository() {
        var repo = new FasterFilesystemForeignSourceRepository();
        repo.setRequisitionPath(opennmsHome + "/etc/imports");
        repo.setForeignSourcePath(opennmsHome + "/etc/foreign-sources");
        return repo;
    }

    @Bean
    @Qualifier("fused")
    public ForeignSourceRepository fusedForeignSourceRepository(
            @Qualifier("filePending") ForeignSourceRepository pending,
            @Qualifier("fileDeployed") ForeignSourceRepository deployed) {
        var repo = new FusedForeignSourceRepository();
        repo.setPendingForeignSourceRepository(pending);
        repo.setDeployedForeignSourceRepository(deployed);
        return repo;
    }

    @Bean
    @Qualifier("fastFused")
    public ForeignSourceRepository fastFusedForeignSourceRepository(
            @Qualifier("fastFilePending") ForeignSourceRepository pending,
            @Qualifier("fastFileDeployed") ForeignSourceRepository deployed) {
        var repo = new FusedForeignSourceRepository();
        repo.setPendingForeignSourceRepository(pending);
        repo.setDeployedForeignSourceRepository(deployed);
        return repo;
    }

    // ===================================================================
    // Section 9: Thread Pools
    // ===================================================================

    @Bean(name = "importExecutor")
    public ScheduledExecutorFactoryBean importExecutor(ProvisiondConfigurationDao configDao) throws IOException {
        var factory = new ScheduledExecutorFactoryBean();
        factory.setPoolSize(configDao.getImportThreads());
        return factory;
    }

    @Bean(name = "scanExecutor")
    public ScheduledExecutorFactoryBean scanExecutor(ProvisiondConfigurationDao configDao) throws IOException {
        var factory = new ScheduledExecutorFactoryBean();
        factory.setPoolSize(configDao.getScanThreads());
        return factory;
    }

    @Bean(name = "writeExecutor")
    public ScheduledExecutorFactoryBean writeExecutor(ProvisiondConfigurationDao configDao) throws IOException {
        var factory = new ScheduledExecutorFactoryBean();
        factory.setPoolSize(configDao.getWriteThreads());
        return factory;
    }

    @Bean(name = "scheduledExecutor")
    public PausibleScheduledThreadPoolExecutor scheduledExecutor(ProvisiondConfigurationDao configDao) throws IOException {
        var threadFactory = new CustomizableThreadFactory();
        threadFactory.setThreadNamePrefix("nodeScanExecutor-");
        return new PausibleScheduledThreadPoolExecutor(configDao.getRescanThreads(), threadFactory);
    }

    // ===================================================================
    // Section 10: Task Coordinator
    // ===================================================================

    @Bean
    public DefaultTaskCoordinator taskCoordinator(
            @Qualifier("importExecutor") ScheduledExecutorService importExec,
            @Qualifier("scanExecutor") ScheduledExecutorService scanExec,
            @Qualifier("writeExecutor") ScheduledExecutorService writeExec) {
        var coordinator = new DefaultTaskCoordinator("Provisiond");
        coordinator.setDefaultExecutor("scan");
        coordinator.setExecutors(Map.of(
            "import", importExec,
            "scan", scanExec,
            "write", writeExec
        ));
        return coordinator;
    }

    // ===================================================================
    // Section 11: Lifecycle Repository
    // ===================================================================

    @Bean
    public LifeCycleRepository lifeCycleRepository(DefaultTaskCoordinator taskCoordinator) {
        var repo = new DefaultLifeCycleRepository(taskCoordinator);
        repo.setLifeCycles(List.of(
            new LifeCycle("import", List.of(
                "validate", "audit", "scan", "delete", "update", "insert", "relate")),
            new LifeCycle("nodeImport", List.of(
                "scan", "persist"))
        ));
        return repo;
    }

    // ===================================================================
    // Section 12: Core Provisiond Beans
    // ===================================================================

    // EntityScopeProvider is provided by DaemonProvisioningConfiguration (@ConditionalOnMissingBean)
    // as NoOpEntityScopeProvider. When real MATE support is needed, override here.

    @Bean
    public JSR223ScriptCache scriptCache() {
        return new JSR223ScriptCache();
    }

    @Bean
    public DefaultPluginRegistry pluginRegistry(
            ServiceRegistry serviceRegistry,
            ApplicationContext applicationContext) {
        return new DefaultPluginRegistry(
            serviceRegistry, applicationContext,
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    @Bean
    public DefaultProvisionService provisionService(
            MonitoringLocationDao monitoringLocationDao,
            NodeDao nodeDao,
            IpInterfaceDao ipInterfaceDao,
            SnmpInterfaceDao snmpInterfaceDao,
            MonitoredServiceDao monitoredServiceDao,
            ServiceTypeDao serviceTypeDao,
            CategoryDao categoryDao,
            RequisitionedCategoryAssociationDao categoryAssociationDao,
            @Qualifier("transactionAware") EventForwarder eventForwarder,
            @Qualifier("fastFused") ForeignSourceRepository fusedRepo,
            @Qualifier("fastFilePending") ForeignSourceRepository pendingRepo,
            DefaultPluginRegistry pluginRegistry,
            PlatformTransactionManager transactionManager,
            LocationAwareDetectorClient locationAwareDetectorClient,
            LocationAwareDnsLookupClient locationAwareDnsLookupClient,
            LocationAwareSnmpClient locationAwareSnmpClient,
            SnmpProfileMapper snmpProfileMapper) {
        return new DefaultProvisionService(
            monitoringLocationDao, nodeDao, ipInterfaceDao, snmpInterfaceDao,
            monitoredServiceDao, serviceTypeDao, categoryDao, categoryAssociationDao,
            eventForwarder, fusedRepo, pendingRepo, pluginRegistry,
            transactionManager, locationAwareDetectorClient,
            locationAwareDnsLookupClient, locationAwareSnmpClient, snmpProfileMapper);
    }

    @Bean
    public CoreImportActivities coreImportActivities(DefaultProvisionService provisionService) {
        return new CoreImportActivities(provisionService);
    }

    @Bean
    public MonitorHolder monitorHolder() {
        return new MonitorHolder();
    }

    @Bean
    public ProvisioningAdapterManager adapterManager(
            DefaultPluginRegistry pluginRegistry,
            EventForwarder eventForwarder) {
        var manager = new ProvisioningAdapterManager();
        manager.setPluginRegistry(pluginRegistry);
        manager.setEventForwarder(eventForwarder);
        return manager;
    }

    // ===================================================================
    // Section 13: Import Scheduler (Quartz)
    // ===================================================================

    @Bean
    public ImportJobFactory importJobFactory(
            MonitorHolder monitorHolder,
            EntityScopeProvider entityScopeProvider) {
        return new ImportJobFactory(monitorHolder, entityScopeProvider);
    }

    @Bean
    public ImportScheduler importScheduler(
            Scheduler scheduler,
            ProvisiondConfigurationDao configDao,
            ImportJobFactory importJobFactory) {
        var importScheduler = new ImportScheduler(scheduler, configDao);
        importScheduler.setImportJobFactory(importJobFactory);
        return importScheduler;
    }

    // ===================================================================
    // Section 14: Provisioner Daemon
    // ===================================================================

    @Bean
    public Provisioner provisioner(
            DefaultProvisionService provisionService,
            @Qualifier("transactionAware") EventForwarder eventForwarder,
            LifeCycleRepository lifeCycleRepository,
            @Qualifier("scheduledExecutor") PausibleScheduledThreadPoolExecutor scheduledExecutor,
            ImportScheduler importScheduler,
            CoreImportActivities importActivities,
            DefaultTaskCoordinator taskCoordinator,
            SnmpAgentConfigFactory agentConfigFactory,
            ProvisioningAdapterManager adapterManager,
            MonitoringSystemDao monitoringSystemDao,
            org.opennms.core.tracing.api.TracerRegistry tracerRegistry,
            MonitorHolder monitorHolder,
            ImportJobFactory importJobFactory) {
        var provisioner = new Provisioner(
            provisionService, eventForwarder, lifeCycleRepository,
            scheduledExecutor, importScheduler, importActivities,
            taskCoordinator, agentConfigFactory, adapterManager,
            monitoringSystemDao, tracerRegistry, monitorHolder);
        // Resolve circular dependencies
        importScheduler.setProvisioner(provisioner);
        importJobFactory.setProvisioner(provisioner);
        return provisioner;
    }

    // ===================================================================
    // Section 15: Lifecycle
    // ===================================================================

    @Bean
    public SmartLifecycle provisiondLifecycle(Provisioner provisioner) {
        return new SpringServiceDaemonSmartLifecycle(provisioner, "Provisiond");
    }

    // ===================================================================
    // Section 16: Event Listeners
    // ===================================================================

    @Bean
    public AnnotationBasedEventListenerAdapter provisiondEventListener(
            Provisioner provisioner,
            @Qualifier("kafkaEventSubscriptionService") EventSubscriptionService eventSubscriptionService) {
        var adapter = new AnnotationBasedEventListenerAdapter();
        adapter.setAnnotatedListener(provisioner);
        adapter.setEventSubscriptionService(eventSubscriptionService);
        return adapter;
    }

    @Bean
    public AnnotationBasedEventListenerAdapter adapterManagerEventListener(
            ProvisioningAdapterManager adapterManager,
            @Qualifier("kafkaEventSubscriptionService") EventSubscriptionService eventSubscriptionService) {
        var adapter = new AnnotationBasedEventListenerAdapter();
        adapter.setAnnotatedListener(adapterManager);
        adapter.setEventSubscriptionService(eventSubscriptionService);
        return adapter;
    }

    // ===================================================================
    // Section 17: SNMP Hardware Inventory Provisioning Adapter
    // DISABLED: HwEntityAttributeType entity not recognized by Hibernate 7
    // (javax.persistence @Entity on classpath but not registered in metamodel).
    // Needs investigation — possibly requires jakarta-transform of opennms-model
    // or explicit entity class registration workaround.
    // ===================================================================

    // ===================================================================
    // Section 18: SNMP Asset Provisioning Adapter
    // ===================================================================

    @Bean
    public SnmpAssetAdapterConfigFactory snmpAssetAdapterConfigFactory() throws IOException {
        return new SnmpAssetAdapterConfigFactory();
    }

    @Bean
    public SnmpAssetAdapterConfig snmpAssetAdapterConfig(
            SnmpAssetAdapterConfigFactory factory) {
        return factory.getInstance();
    }

    @Bean
    public SnmpAssetProvisioningAdapter snmpAssetProvisioningAdapter(
            NodeDao nodeDao,
            EventForwarder eventForwarder,
            SnmpAssetAdapterConfig snmpAssetConfig,
            SnmpAgentConfigFactory snmpPeerFactory,
            LocationAwareSnmpClient locationAwareSnmpClient,
            TransactionTemplate transactionTemplate) {
        var adapter = new SnmpAssetProvisioningAdapter();
        adapter.setNodeDao(nodeDao);
        adapter.setEventForwarder(eventForwarder);
        adapter.setSnmpAssetAdapterConfig(snmpAssetConfig);
        adapter.setSnmpPeerFactory(snmpPeerFactory);
        adapter.setLocationAwareSnmpClient(locationAwareSnmpClient);
        adapter.setTemplate(transactionTemplate);
        return adapter;
    }

    @Bean
    public AnnotationBasedEventListenerAdapter snmpAssetEventListener(
            SnmpAssetProvisioningAdapter adapter,
            @Qualifier("kafkaEventSubscriptionService") EventSubscriptionService eventSubscriptionService) {
        var listener = new AnnotationBasedEventListenerAdapter();
        listener.setAnnotatedListener(adapter);
        listener.setEventSubscriptionService(eventSubscriptionService);
        return listener;
    }

    // ===================================================================
    // Section 19: SNMP Metadata Provisioning Adapter
    // ===================================================================

    @Bean
    public SnmpMetadataConfigDao snmpMetadataConfigDao() {
        var dao = new SnmpMetadataConfigDao();
        dao.setConfigResource(
            new FileSystemResource(opennmsHome + "/etc/snmp-metadata-adapter-configuration.xml"));
        dao.afterPropertiesSet();
        return dao;
    }

    @Bean
    public SnmpMetadataProvisioningAdapter snmpMetadataProvisioningAdapter(
            NodeDao nodeDao,
            SnmpAgentConfigFactory snmpPeerFactory,
            LocationAwareSnmpClient locationAwareSnmpClient,
            EventForwarder eventForwarder,
            SnmpMetadataConfigDao snmpMetadataConfigDao,
            TransactionTemplate transactionTemplate) {
        var adapter = new SnmpMetadataProvisioningAdapter();
        adapter.setNodeDao(nodeDao);
        adapter.setSnmpConfigDao(snmpPeerFactory);
        adapter.setLocationAwareSnmpClient(locationAwareSnmpClient);
        adapter.setEventForwarder(eventForwarder);
        adapter.setSnmpMetadataAdapterConfigDao(snmpMetadataConfigDao);
        adapter.setTemplate(transactionTemplate);
        return adapter;
    }

    @Bean
    public AnnotationBasedEventListenerAdapter snmpMetadataEventListener(
            SnmpMetadataProvisioningAdapter adapter,
            @Qualifier("kafkaEventSubscriptionService") EventSubscriptionService eventSubscriptionService) {
        var listener = new AnnotationBasedEventListenerAdapter();
        listener.setAnnotatedListener(adapter);
        listener.setEventSubscriptionService(eventSubscriptionService);
        return listener;
    }
}
