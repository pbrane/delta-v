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
package org.deltav.netmgt.perspectivepoller.boot;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import io.micrometer.core.instrument.MeterRegistry;

import org.deltav.poller.catalog.Catalog;
import org.deltav.poller.catalog.CatalogParser;
import org.deltav.poller.catalog.CatalogTranslator;
import org.deltav.poller.catalog.CatalogValidator;
import org.deltav.poller.catalog.EngineSettings;
import org.deltav.poller.catalog.InventoryFilterDao;
import org.deltav.poller.catalog.ValidationMessage;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.netmgt.collection.api.CollectionAgentFactory;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.config.PollerConfig;
import org.opennms.netmgt.config.PollerConfigFactory;
import org.opennms.netmgt.config.SnmpPeerFactory;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.snmp.SnmpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opennms.netmgt.dao.api.ApplicationDao;
import org.opennms.netmgt.dao.api.IpInterfaceDao;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.MonitoringLocationDao;
import org.opennms.netmgt.dao.api.OutageDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.events.api.AnnotationBasedEventListenerAdapter;
import org.opennms.netmgt.perspectivepoller.PerspectivePollerd;
import org.opennms.netmgt.perspectivepoller.PerspectiveServiceTracker;
import org.opennms.netmgt.poller.LocationAwarePollerClient;
import org.opennms.netmgt.poller.ServiceMonitorRegistry;
import org.opennms.netmgt.threshd.api.ThresholdingService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.deltav.poller.timeseries.ResponseTimePublisher;
import org.springframework.context.SmartLifecycle;

/**
 * Spring Boot configuration for the PerspectivePollerd daemon and its core dependencies.
 *
 * <p>Wires the {@link PerspectivePollerd} daemon with its configuration,
 * service tracker, event adapters, and lifecycle management. The daemon is
 * started via {@link SpringServiceDaemonSmartLifecycle} which calls
 * {@code afterPropertiesSet()} then {@code start()}.</p>
 *
 * <p>PerspectivePollerd polls services from perspective (remote) monitoring
 * locations to detect location-specific outages. It shares the flat poller service
 * catalog ({@code etc/poller-services.yaml}) with Pollerd but uses its own scheduling
 * (driven by application membership via {@code ApplicationDao}) and outage tracking.</p>
 *
 * <p>The {@link PollerConfigFactory} is built from the flat catalog translated into the
 * synthetic config the frozen engine expects, with a constructor-injected
 * {@link InventoryFilterDao} replacing the legacy {@code JdbcFilterDao}/{@code FilterDaoFactory}/
 * DB-schema coupling (FR7). The frozen {@code PerspectivePollerd.onServicePerspectiveAdded} DOES
 * consult the synthetic config — it selects the per-service package via
 * {@code isInterfaceInPackage(ip, pkg)} (matched against the {@link InventoryFilterDao}-built IP map)
 * and {@code isServiceInPackageAndEnabled(serviceName, pkg)}, then looks up the monitor + parameters.
 * The {@link InventoryFilterDao} catch-all over active inventory IPs reproduces the legacy
 * {@code JdbcFilterDao} active-IP set, so behavior is unchanged. A perspective-mapped service absent
 * from the catalog (or whose monitor is unregistered) is silently dropped by the engine — surfaced by
 * the FR9 {@link PerspectiveCatalogStartupCheck} gauge.</p>
 */
@Configuration
public class PerspectivePollerdDaemonConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(PerspectivePollerdDaemonConfiguration.class);

    private static final XmlMapper XML_MAPPER;
    static {
        XML_MAPPER = XmlMapper.builder().defaultUseWrapper(false).build();
        XML_MAPPER.registerModule(new JaxbAnnotationModule());
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Value("${opennms.home:/opt/deltav}")
    private String opennmsHome;

    // Engine-tuning scalars (FR1b three-way split): not monitoring semantics, so they live in
    // application.yml rather than the catalog file. Defaults mirror the legacy poller-configuration.xml
    // root attributes (threads=30, asyncPollingEngineEnabled=false, maxConcurrentAsyncPolls=200).
    @Value("${poller.engine.threads:30}")
    private int pollerThreads;

    @Value("${poller.engine.async-polling-engine-enabled:false}")
    private boolean pollerAsyncPollingEngineEnabled;

    @Value("${poller.engine.max-concurrent-async-polls:200}")
    private int pollerMaxConcurrentAsyncPolls;

    /**
     * Initializes the SNMP peer factory from snmp-config.xml via Jackson XmlMapper.
     * Required by SnmpMonitorStrategy.getRuntimeAttributes() which calls
     * SnmpPeerFactory.getInstance().getAgentConfig() to resolve SNMP
     * credentials for each polled service.
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
     * Translates the flat poller service catalog into the synthetic JAXB
     * {@link org.opennms.netmgt.config.poller.PollerConfiguration} the frozen engine expects and
     * creates a {@link PollerConfigFactory} backed by an {@link InventoryFilterDao} (FR7 — no
     * {@code JdbcFilterDao}/{@code FilterDaoFactory}/DB-schema coupling).
     *
     * <p>The set of polled {@code (service, perspective-location)} tuples is application-membership
     * driven ({@code ApplicationDao}), but for each such service the engine still resolves its package
     * from the synthetic config via {@code isInterfaceInPackage} (over the {@link InventoryFilterDao}
     * IP map) and {@code isServiceInPackageAndEnabled}, then reads the monitor + parameters — so the
     * catalog and the catch-all FilterDao are both on the perspective scheduling path, reproducing the
     * legacy active-IP behavior. The {@code setPollerConfigFile} + matching {@code lastModified} version
     * neutralizes the manager's reload guard so the (non-XML) YAML is never parsed over the synthetic
     * config.</p>
     */
    @Bean
    public PollerConfig pollerConfig(Catalog perspectiveCatalog, IpInterfaceDao ipInterfaceDao,
            SessionUtils sessionUtils) throws IOException {
        var catalogFile = new File(opennmsHome, "etc/poller-services.yaml");

        var settings = new EngineSettings(pollerThreads, pollerAsyncPollingEngineEnabled, pollerMaxConcurrentAsyncPolls);
        var config = new CatalogTranslator().translate(perspectiveCatalog, settings);

        var filterDao = new InventoryFilterDao(() -> activeInventoryIps(ipInterfaceDao, sessionUtils));

        PollerConfigFactory.setPollerConfigFile(catalogFile);
        var factory = new PollerConfigFactory(catalogFile.lastModified(), config, filterDao);
        PollerConfigFactory.setInstance(factory);
        return factory;
    }

    /**
     * Parses and validates the flat catalog once. Refusing to start on any validation ERROR keeps a
     * malformed catalog from silently unmonitoring services (defense-in-depth behind the build-time
     * lint).
     */
    @Bean
    public Catalog perspectiveCatalog() throws IOException {
        var catalogFile = new File(opennmsHome, "etc/poller-services.yaml");
        LOG.info("Loading flat poller service catalog from {}", catalogFile);
        var catalog = new CatalogParser().parse(catalogFile.toPath());
        failOnValidationErrors(catalogFile, new CatalogValidator().validate(catalog));
        return catalog;
    }

    /**
     * Config-gap observability (FR9): publishes the {@code deltav_perspectivepollerd_services_unscheduled}
     * gauge and a one-line {@code perspective-catalog-summary} for the perspective-polled service types
     * (application members) perspectivepollerd cannot schedule. Unlike pollerd's inventory-wide check, the
     * universe here is {@code ApplicationDao.getServicePerspectives()} — the services PerspectivePollerd
     * actually attempts to poll. See {@link PerspectiveCatalogStartupCheck}.
     */
    @Bean
    public PerspectiveCatalogStartupCheck perspectiveCatalogStartupCheck(Catalog perspectiveCatalog,
                                                                         ServiceMonitorRegistry serviceMonitorRegistry,
                                                                         ApplicationDao applicationDao,
                                                                         SessionUtils sessionUtils,
                                                                         MeterRegistry meterRegistry) {
        return new PerspectiveCatalogStartupCheck(perspectiveCatalog, serviceMonitorRegistry, applicationDao,
                sessionUtils, meterRegistry);
    }

    /**
     * Tracks perspective-eligible services by monitoring application membership
     * changes via events and periodic refresh.
     */
    @Bean
    public PerspectiveServiceTracker perspectiveServiceTracker(
            SessionUtils sessionUtils,
            ApplicationDao applicationDao) {
        return new PerspectiveServiceTracker(sessionUtils, applicationDao);
    }

    /**
     * The PerspectivePollerd daemon — wired as {@link InstrumentedPerspectivePollerd}
     * so per-poll {@code deltav_perspective_*} counters fire and (when
     * {@code deltav.perspective.timeseries.enabled=true}) response-time
     * samples publish to the {@code deltav-timeseries} Kafka topic.
     *
     * <p>The bean's declared type stays {@link PerspectivePollerd} so
     * downstream consumers ({@link AnnotationBasedEventListenerAdapter},
     * the SmartLifecycle wrapper) bind unchanged.</p>
     *
     * <p>Constructor parameter 9 ({@code eventForwarder}) is typed as
     * {@code EventForwarder}, but {@code EventIpcManager} extends
     * {@code EventForwarder}, so passing the EventIpcManager bean is valid.
     * The forwarder slot is wrapped with {@link CountingPerspectiveEventForwarder}
     * so per-perspective lifecycle UEIs (nodeLostService /
     * nodeRegainedService) surface at {@code /actuator/prometheus}. The
     * unwrapped {@code EventIpcManager} bean is still used by the
     * {@code AnnotationBasedEventListenerAdapter} beans for inbound
     * subscription registration.</p>
     *
     * <p>The publisher is supplied via {@link ObjectProvider} because
     * {@link PerspectivePollerdTimeseriesConfiguration} is gated on a
     * daemon-scoped flag; when disabled, no bean is registered and Phase 3
     * publishing is a no-op.</p>
     */
    @Bean
    public PerspectivePollerd perspectivePollerd(
            SessionUtils sessionUtils,
            MonitoringLocationDao monitoringLocationDao,
            PollerConfig pollerConfig,
            MonitoredServiceDao monitoredServiceDao,
            LocationAwarePollerClient locationAwarePollerClient,
            ApplicationDao applicationDao,
            CollectionAgentFactory collectionAgentFactory,
            PersisterFactory persisterFactory,
            EventIpcManager eventIpcManager,
            ThresholdingService thresholdingService,
            OutageDao outageDao,
            TracerRegistry tracerRegistry,
            PerspectiveServiceTracker perspectiveServiceTracker,
            io.micrometer.core.instrument.MeterRegistry meterRegistry,
            ObjectProvider<ResponseTimePublisher> publisherProvider) {
        EventForwarder countingForwarder =
                new CountingPerspectiveEventForwarder(eventIpcManager, meterRegistry);
        ResponseTimePublisher publisher = publisherProvider.getIfAvailable();
        return new InstrumentedPerspectivePollerd(sessionUtils, monitoringLocationDao, pollerConfig,
                monitoredServiceDao, locationAwarePollerClient, applicationDao,
                collectionAgentFactory, persisterFactory, countingForwarder,
                thresholdingService, outageDao, tracerRegistry, perspectiveServiceTracker,
                meterRegistry, publisher);
    }

    /**
     * Registers PerspectivePollerd's @EventHandler methods with the EventIpcManager.
     */
    @Bean
    public AnnotationBasedEventListenerAdapter perspectivePollerdEventAdapter(
            PerspectivePollerd perspectivePollerd,
            EventIpcManager eventIpcManager) {
        var adapter = new AnnotationBasedEventListenerAdapter();
        adapter.setAnnotatedListener(perspectivePollerd);
        adapter.setEventSubscriptionService(eventIpcManager);
        return adapter;
    }

    /**
     * Registers PerspectiveServiceTracker's @EventHandler methods with the EventIpcManager.
     */
    @Bean
    public AnnotationBasedEventListenerAdapter perspectiveServiceTrackerEventAdapter(
            PerspectiveServiceTracker perspectiveServiceTracker,
            EventIpcManager eventIpcManager) {
        var adapter = new AnnotationBasedEventListenerAdapter();
        adapter.setAnnotatedListener(perspectiveServiceTracker);
        adapter.setEventSubscriptionService(eventIpcManager);
        return adapter;
    }

    @Bean
    public SmartLifecycle perspectivePollerdLifecycle(PerspectivePollerd perspectivePollerd) {
        return new SpringServiceDaemonSmartLifecycle(perspectivePollerd, "PerspectivePollerd");
    }

    /**
     * Logs every validation finding and refuses to start if any is an ERROR. The build-time lint
     * already gates the image; this is defense-in-depth so a malformed catalog fails the context
     * loudly rather than silently unmonitoring services.
     */
    private static void failOnValidationErrors(File catalogFile, List<ValidationMessage> messages) {
        boolean hasError = false;
        for (var message : messages) {
            if (message.isError()) {
                LOG.error("{}", message.format(catalogFile.getName()));
                hasError = true;
            } else {
                LOG.warn("{}", message.format(catalogFile.getName()));
            }
        }
        if (hasError) {
            throw new IllegalStateException(
                    "Poller service catalog " + catalogFile + " has validation errors; refusing to start");
        }
    }

    /**
     * Every non-deleted inventory IP address, queried lazily inside a read-only transaction.
     * Replicates the legacy active-IP set ({@code ipInterface.isManaged != 'D'}); the synthetic
     * catalog packages have no filter rules, so this is the complete polled-interface universe.
     */
    private static List<InetAddress> activeInventoryIps(IpInterfaceDao ipInterfaceDao, SessionUtils sessionUtils) {
        return sessionUtils.withReadOnlyTransaction(() ->
                ipInterfaceDao.findAll().stream()
                        .filter(iface -> !"D".equals(iface.getIsManaged()))
                        .map(OnmsIpInterface::getIpAddress)
                        .filter(Objects::nonNull)
                        .toList());
    }
}
