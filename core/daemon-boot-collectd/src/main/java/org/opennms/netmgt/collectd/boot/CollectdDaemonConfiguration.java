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
package org.opennms.netmgt.collectd.boot;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import org.opennms.core.cache.CacheConfig;
import org.opennms.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.features.distributed.kvstore.json.noop.NoOpJsonStore;
import org.opennms.features.timeseries.plugin.InMemoryStorage;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.collectd.Collectd;
import org.opennms.netmgt.collectd.DefaultResourceTypeMapper;
import org.opennms.netmgt.collectd.DefaultSnmpCollectionAgentFactory;
import org.opennms.netmgt.config.api.CollectdConfigFactory;
import org.opennms.netmgt.config.api.DefaultCollectdConfigFactory;
import org.opennms.netmgt.config.collectd.CollectdConfiguration;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.config.DataCollectionConfigFactory;
import org.opennms.netmgt.config.DefaultDataCollectionConfigDao;
import org.opennms.netmgt.config.DefaultResourceTypesDao;
import org.opennms.netmgt.config.SnmpPeerFactory;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.snmp.SnmpConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.opennms.netmgt.config.dao.outages.api.ReadablePollOutagesDao;
import org.opennms.netmgt.config.dao.outages.impl.OnmsPollOutagesDao;
import org.opennms.netmgt.dao.api.NodeDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.events.api.EventIpcManagerFactory;
import org.opennms.netmgt.threshd.api.ThresholdInitializationException;
import org.opennms.netmgt.threshd.api.ThresholdingService;
import org.opennms.netmgt.threshd.api.ThresholdingSession;
import org.opennms.netmgt.threshd.api.ThresholdingSetPersister;
import org.opennms.netmgt.collection.api.ServiceParameters;
import org.opennms.netmgt.timeseries.TimeseriesStorageManager;
import org.opennms.netmgt.timeseries.TimeseriesStorageManagerImpl;
import org.opennms.netmgt.timeseries.samplewrite.MetaTagDataLoader;
import org.opennms.netmgt.timeseries.samplewrite.TimeseriesPersisterFactory;
import org.opennms.netmgt.timeseries.samplewrite.TimeseriesWriterConfig;
import org.opennms.netmgt.timeseries.stats.StatisticsCollector;
import org.opennms.netmgt.timeseries.stats.StatisticsCollectorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.codahale.metrics.MetricRegistry;

/**
 * Spring Boot configuration for the Collectd daemon and its core dependencies.
 *
 * <p>Wires the {@link Collectd} daemon with its configuration factories,
 * resource type mapping, collection agent factory, time-series persistence
 * pipeline, and lifecycle management.</p>
 *
 * <p>The TSS (Time Series Storage) persistence pipeline is the key novelty
 * compared to other daemon configurations. It wires the full chain from
 * {@link InMemoryStorage} through {@link TimeseriesStorageManagerImpl},
 * {@link TimeseriesWriterConfig}, {@link MetaTagDataLoader}, and finally
 * {@link TimeseriesPersisterFactory} to produce a {@link PersisterFactory}
 * that Collectd uses to persist collected metrics.</p>
 *
 * <p>Event handling is self-registered: Collectd registers itself as an
 * event listener via {@code getEventIpcManager().addEventListener(this, ueiList)}
 * inside {@code onInit()}. No AnnotationBasedEventListenerAdapter is needed.</p>
 *
 * <p>Bean ordering: {@link CollectdConfigFactory} calls
 * {@code FilterDaoFactory.getInstance()} internally, so the
 * {@code filterDaoInitializer} bean in {@link CollectdJpaConfiguration}
 * must be initialized first. This is enforced via {@code @DependsOn}.</p>
 */
@Configuration
public class CollectdDaemonConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(CollectdDaemonConfiguration.class);

    private static final XmlMapper XML_MAPPER;
    static {
        XML_MAPPER = XmlMapper.builder().defaultUseWrapper(false).build();
        XML_MAPPER.registerModule(new JaxbAnnotationModule());
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Value("${opennms.home:/opt/deltav}")
    private String opennmsHome;

    // ===================================================================
    // Section 1: Config Factories
    // ===================================================================

    /**
     * Loads collectd-configuration.xml via Jackson XmlMapper and creates a
     * {@link DefaultCollectdConfigFactory} with injected FilterDao.
     *
     * <p>Must run after FilterDaoFactory initialization because
     * filter evaluation in package matching requires an active FilterDao.</p>
     */
    @Bean
    @DependsOn("filterDaoInitializer")
    public CollectdConfigFactory collectdConfigFactory(FilterDao filterDao) throws IOException {
        var configFile = new File(opennmsHome, "etc/collectd-configuration.xml");
        LOG.info("Loading CollectdConfigFactory from {}", configFile);
        var config = XML_MAPPER.readValue(configFile, CollectdConfiguration.class);
        return new DefaultCollectdConfigFactory(config, filterDao);
    }

    /**
     * Initializes the SNMP peer factory from snmp-config.xml via Jackson XmlMapper.
     */
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
     * Initializes the DataCollectionConfigFactory singleton.
     * Loads datacollection-config.xml which defines SNMP OIDs and groups to collect.
     */
    @Bean
    public DefaultDataCollectionConfigDao dataCollectionConfigDao(
            @Value("${opennms.home:/opt/deltav}") String opennmsHome) throws IOException {
        var configFile = new File(opennmsHome, "etc/datacollection-config.xml");
        LOG.info("Initializing DataCollectionConfigFactory from {}", configFile);
        var dao = new DefaultDataCollectionConfigDao();
        dao.setConfigResource(new FileSystemResource(configFile));
        dao.setConfigDirectory(new File(opennmsHome, "etc/datacollection").getAbsolutePath());
        dao.afterPropertiesSet();
        DataCollectionConfigFactory.setInstance(dao);
        return dao;
    }

    // ===================================================================
    // Section 2: Resource Type Mapper
    // ===================================================================

    /**
     * DAO that loads custom resource type definitions from datacollection/
     * resource-types configuration files.
     */
    @Bean
    public DefaultResourceTypesDao resourceTypesDao() {
        return new DefaultResourceTypesDao();
    }

    /**
     * Registers a resource type lookup function with the static
     * {@link org.opennms.netmgt.collection.api.ResourceTypeMapper} singleton.
     *
     * <p>{@link DefaultResourceTypeMapper} has {@code @Autowired ResourceTypesDao}
     * and {@code @PostConstruct registerWithTypeMapper()}, both of which Spring
     * handles automatically.</p>
     */
    @Bean
    public DefaultResourceTypeMapper defaultResourceTypeMapper() {
        return new DefaultResourceTypeMapper();
    }

    // ===================================================================
    // Section 3: Collection Agent Factory
    // ===================================================================

    /**
     * Factory for creating SNMP collection agents.
     *
     * <p>{@link DefaultSnmpCollectionAgentFactory} extends
     * {@code AbstractCollectionAgentFactory} which has {@code @Autowired}
     * fields for {@code IpInterfaceDao} and {@code PlatformTransactionManager},
     * both satisfied by Spring from the JPA configuration.</p>
     */
    @Bean
    public DefaultSnmpCollectionAgentFactory collectionAgentFactory() {
        return new DefaultSnmpCollectionAgentFactory();
    }

    // ===================================================================
    // Section 4: Poll Outages
    // ===================================================================

    /**
     * Loads poll-outages.xml via {@link OnmsPollOutagesDao}.
     *
     * <p>Uses a {@link NoOpJsonStore} because standalone Collectd does not
     * need distributed config synchronization -- it reads directly from
     * the local filesystem.</p>
     */
    @Bean
    public ReadablePollOutagesDao pollOutagesDao() throws IOException {
        LOG.info("Initializing OnmsPollOutagesDao with NoOpJsonStore");
        return new OnmsPollOutagesDao(new NoOpJsonStore());
    }

    // ===================================================================
    // Section 5: TSS Persistence Pipeline
    // ===================================================================

    /**
     * In-memory time series storage implementation.
     *
     * <p>Default storage for standalone Collectd. Suitable for testing and
     * development. Production deployments should configure a real TSS plugin
     * (e.g., Cortex, TimescaleDB) by setting {@code opennms.timeseries.strategy}
     * to a value other than {@code inmemory}.</p>
     */
    @Bean
    @ConditionalOnProperty(name = "opennms.timeseries.strategy", havingValue = "inmemory", matchIfMissing = true)
    public TimeSeriesStorage inMemoryStorage() {
        LOG.info("Using InMemoryStorage for time series persistence");
        return new InMemoryStorage();
    }

    /**
     * Wraps the {@link TimeSeriesStorage} SPI implementation.
     *
     * <p>The default constructor uses OSGi ServiceLookup which we bypass by
     * calling {@code onBind()} directly to register the storage. The
     * {@code get()} method checks {@code stackOfStorages} first before
     * falling back to OSGi lookup.</p>
     */
    @Bean
    public TimeseriesStorageManager timeseriesStorageManager(TimeSeriesStorage storage) {
        TimeseriesStorageManagerImpl manager = new TimeseriesStorageManagerImpl();
        manager.onBind(storage, Map.of());
        return manager;
    }

    /**
     * Buffer configuration for the time series writer.
     */
    @Bean
    public TimeseriesWriterConfig timeseriesWriterConfig() {
        TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setBufferSize(8192);
        config.setNumWriterThreads(16);
        return config;
    }

    /**
     * Statistics collector for time series write metrics.
     */
    @Bean
    public StatisticsCollector statisticsCollector() {
        return new StatisticsCollectorImpl(Runtime.getRuntime().availableProcessors());
    }

    // Reuse kafkaRpcMetricRegistry from daemon-common — no separate MetricRegistry needed.
    // Having two MetricRegistry beans causes autowiring ambiguity in KafkaRpcClientFactory.

    /**
     * Cache configuration for meta-tag resolution in the persister.
     */
    @Bean
    public CacheConfig timeseriesPersisterMetaTagCache() {
        CacheConfig config = new CacheConfig("timeseriesPersisterMetaTagCache");
        config.setMaximumSize(8192L);
        config.setExpireAfterWrite(300L);
        return config;
    }

    /**
     * Loads meta-tag data (node-level and interface-level tags) for
     * enriching time series metrics with additional metadata.
     */
    @Bean
    public MetaTagDataLoader metaTagDataLoader(NodeDao nodeDao, SessionUtils sessionUtils,
                                                EntityScopeProvider entityScopeProvider) {
        return new MetaTagDataLoader(nodeDao, sessionUtils, entityScopeProvider);
    }

    /**
     * Time series persister factory -- the core of the persistence pipeline.
     *
     * <p>Wires together the full chain: {@link MetaTagDataLoader} for tag enrichment,
     * {@link StatisticsCollector} for write metrics, {@link TimeseriesStorageManager}
     * for the storage backend, cache configuration, Codahale metrics, and the
     * writer config (buffer size, thread count).</p>
     *
     * <p>{@link TimeseriesPersisterFactory} constructor uses {@code @Named}
     * annotations for disambiguation, which we satisfy by passing the correct
     * beans explicitly.</p>
     */
    @Bean
    public PersisterFactory persisterFactory(MetaTagDataLoader metaTagDataLoader,
                                              StatisticsCollector statisticsCollector,
                                              TimeseriesStorageManager timeseriesStorageManager,
                                              CacheConfig timeseriesPersisterMetaTagCache,
                                              MetricRegistry kafkaRpcMetricRegistry,
                                              TimeseriesWriterConfig timeseriesWriterConfig) {
        return new TimeseriesPersisterFactory(metaTagDataLoader, statisticsCollector,
                timeseriesStorageManager, timeseriesPersisterMetaTagCache,
                kafkaRpcMetricRegistry, timeseriesWriterConfig);
    }

    // ===================================================================
    // Section 6: No-op ThresholdingService
    // ===================================================================

    /**
     * No-op thresholding service.
     *
     * <p>Collectd requires a {@link ThresholdingService} for threshold evaluation
     * during collection. In standalone mode, thresholding is not yet supported
     * (it requires the full thresholding engine and its configuration). This
     * no-op implementation allows collections to proceed without thresholding.</p>
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

    // ===================================================================
    // Section 7: Collectd Daemon
    // ===================================================================

    /**
     * The Collectd daemon.
     *
     * <p>Collectd has a 0-arg constructor. All dependencies except
     * {@code EventIpcManager} are {@code @Autowired} fields satisfied by Spring
     * auto-wiring. {@code EventIpcManager} is set via the explicit setter
     * because it was historically setter-injected.</p>
     */
    @Bean
    public Collectd collectd(EventIpcManager eventIpcManager) {
        // CollectableService.sendEvent() uses the static EventIpcManagerFactory singleton
        // rather than the Spring-injected EventIpcManager. Initialize it here.
        EventIpcManagerFactory.setIpcManager(eventIpcManager);

        Collectd collectd = new Collectd();
        collectd.setEventIpcManager(eventIpcManager);
        return collectd;
    }

    // ===================================================================
    // Section 8: Lifecycle
    // ===================================================================

    /**
     * Wraps the Collectd daemon in a {@link SmartLifecycle} so Spring Boot
     * manages its startup and shutdown. Phase is {@code Integer.MAX_VALUE}
     * so the daemon starts last (after all infrastructure beans) and stops first.
     */
    @Bean
    public SmartLifecycle collectdLifecycle(Collectd collectd) {
        return new SpringServiceDaemonSmartLifecycle(collectd);
    }
}
