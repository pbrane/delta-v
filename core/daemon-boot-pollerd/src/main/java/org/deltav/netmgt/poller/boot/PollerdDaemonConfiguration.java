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
package org.deltav.netmgt.poller.boot;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.deltav.poller.catalog.Catalog;
import org.deltav.poller.catalog.CatalogParser;
import org.deltav.poller.catalog.CatalogTranslator;
import org.deltav.poller.catalog.CatalogValidator;
import org.deltav.poller.catalog.EngineSettings;
import org.deltav.poller.catalog.InventoryFilterDao;
import org.deltav.poller.catalog.ValidationMessage;
import org.deltav.poller.timeseries.ResponseTimePublisher;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.tsid.TsidFactory;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.features.distributed.kvstore.json.noop.NoOpJsonStore;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.config.PollerConfig;
import org.opennms.netmgt.config.PollerConfigFactory;
import org.opennms.netmgt.config.SnmpPeerFactory;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.snmp.SnmpConfig;
import org.opennms.netmgt.config.dao.outages.api.ReadablePollOutagesDao;
import org.opennms.netmgt.config.dao.outages.impl.OnmsPollOutagesDao;
import org.opennms.netmgt.dao.api.IpInterfaceDao;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.OutageDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.model.OnmsIpInterface;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.icmp.proxy.LocationAwarePingClient;
import org.opennms.netmgt.poller.LocationAwarePollerClient;
import org.opennms.netmgt.poller.Poller;
import org.opennms.netmgt.poller.QueryManager;
import org.opennms.netmgt.poller.ServiceMonitorRegistry;
import org.opennms.netmgt.poller.pollables.PollContext;
import org.opennms.netmgt.poller.pollables.PollableNetwork;
import org.opennms.netmgt.threshd.api.ThresholdingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring Boot configuration for the Poller daemon and its core dependencies.
 *
 * <p>Wires the {@link Poller} daemon with its configuration, poll context,
 * pollable network tree, and lifecycle management. The Poller is started via
 * {@link SpringServiceDaemonSmartLifecycle} which calls {@code init()} then {@code start()}.</p>
 *
 * <p>During {@code init()}, Poller creates a LegacyScheduler, closes outages
 * for unmanaged services, schedules existing services, and creates the
 * PollerEventProcessor event listener. Event handling is self-registered
 * via {@code EventIpcManager.addEventListener()} inside {@code Poller.init()} --
 * no AnnotationBasedEventListenerAdapter bean is needed.</p>
 *
 * <p>The {@link PollerConfigFactory} is built from the flat poller service catalog
 * ({@code etc/poller-services.yaml}) translated into the synthetic config the frozen engine
 * expects, with a constructor-injected {@link InventoryFilterDao} replacing the legacy
 * {@code JdbcFilterDao}/{@code FilterDaoFactory}/DB-schema coupling (FR7).</p>
 */
@Configuration
public class PollerdDaemonConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(PollerdDaemonConfiguration.class);

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
     * Loads the flat poller service catalog ({@code etc/poller-services.yaml}), validates it,
     * translates it into the synthetic JAXB {@link org.opennms.netmgt.config.poller.PollerConfiguration}
     * the frozen engine expects, and creates a {@link PollerConfigFactory} backed by an
     * {@link InventoryFilterDao} (FR7 — no {@code JdbcFilterDao}/{@code FilterDaoFactory}/DB-schema
     * coupling).
     *
     * <p>The {@code setPollerConfigFile} + matching {@code lastModified} version neutralizes
     * {@code update()} (amended D3): the manager's {@code lastModified > version} reload guard is
     * always false, so the (non-XML) YAML is never parsed over the synthetic config. {@code save()}
     * is never wired — it would write XML over the YAML.</p>
     *
     * <p>The {@link InventoryFilterDao} supplier returns every non-deleted inventory IP
     * (matching the legacy {@code JdbcFilterDao.getActiveIPAddressList} catch-all set:
     * {@code isManaged != 'D'}), queried lazily inside a read-only transaction.</p>
     */
    @Bean
    public PollerConfig pollerConfig(Catalog pollerCatalog, IpInterfaceDao ipInterfaceDao, SessionUtils sessionUtils)
            throws IOException {
        var catalogFile = new File(opennmsHome, "etc/poller-services.yaml");

        var settings = new EngineSettings(pollerThreads, pollerAsyncPollingEngineEnabled, pollerMaxConcurrentAsyncPolls);
        var config = new CatalogTranslator().translate(pollerCatalog, settings);

        var filterDao = new InventoryFilterDao(() -> activeInventoryIps(ipInterfaceDao, sessionUtils));

        PollerConfigFactory.setPollerConfigFile(catalogFile);
        var factory = new PollerConfigFactory(catalogFile.lastModified(), config, filterDao);
        PollerConfigFactory.setInstance(factory);
        return factory;
    }

    /**
     * Parses and validates the flat catalog once, shared by {@link #pollerConfig} (which translates
     * it for the engine) and {@link #catalogStartupCheck} (which compares it against inventory).
     * Refusing to start on any validation ERROR keeps a malformed catalog from silently
     * unmonitoring services (defense-in-depth behind the build-time lint).
     */
    @Bean
    public Catalog pollerCatalog() throws IOException {
        var catalogFile = new File(opennmsHome, "etc/poller-services.yaml");
        LOG.info("Loading flat poller service catalog from {}", catalogFile);
        var catalog = new CatalogParser().parse(catalogFile.toPath());
        failOnValidationErrors(catalogFile, new CatalogValidator().validate(catalog));
        return catalog;
    }

    /**
     * Config-gap observability (FR9): publishes the {@code deltav_pollerd_services_unscheduled}
     * gauge and the one-line {@code catalog-summary} for inventory service types pollerd cannot
     * schedule. See {@link CatalogStartupCheck}.
     */
    @Bean
    public CatalogStartupCheck catalogStartupCheck(Catalog pollerCatalog,
                                                   ServiceMonitorRegistry serviceMonitorRegistry,
                                                   MonitoredServiceDao monitoredServiceDao,
                                                   SessionUtils sessionUtils,
                                                   MeterRegistry meterRegistry) {
        return new CatalogStartupCheck(pollerCatalog, serviceMonitorRegistry, monitoredServiceDao,
                sessionUtils, meterRegistry);
    }

    /**
     * Logs every validation finding and refuses to start if any is an ERROR. The build-time lint
     * (Story 1.5) already gates the image; this is defense-in-depth so a malformed catalog fails the
     * context loudly rather than silently unmonitoring services.
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

    /**
     * Loads poll-outages.xml via {@link OnmsPollOutagesDao}.
     *
     * <p>Uses a {@link NoOpJsonStore} because standalone Pollerd does not
     * need distributed config synchronization -- it reads directly from
     * the local filesystem.</p>
     */
    @Bean
    public ReadablePollOutagesDao pollOutagesDao() throws IOException {
        LOG.info("Initializing OnmsPollOutagesDao with NoOpJsonStore");
        return new OnmsPollOutagesDao(new NoOpJsonStore());
    }

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
     * PollContext that skips AsyncPollingEngine creation and instruments
     * every poll with {@code deltav_pollerd_*} Micrometer counters plus
     * (when {@code deltav.timeseries.enabled=true}) per-poll Kafka publish
     * via {@link ResponseTimePublisher}.
     *
     * <p>The {@link ObjectProvider} for {@link ResponseTimePublisher} is used
     * so the bean stays optional — {@code PollerdTimeseriesConfiguration}
     * is gated by {@code @ConditionalOnProperty}, so the publisher is absent
     * unless the flag is on. {@link InstrumentedPollContext} treats a null
     * publisher as "Phase 3 disabled" and still emits Phase 2 counters.</p>
     */
    @Bean
    public PollContext pollContext(EventIpcManager eventIpcManager,
                                  PollerConfig pollerConfig,
                                  QueryManager queryManager,
                                  LocationAwarePingClient locationAwarePingClient,
                                  TsidFactory tsidFactory,
                                  MeterRegistry meterRegistry,
                                  ObjectProvider<ResponseTimePublisher> publisherProvider) {
        String localHostName = InetAddressUtils.getLocalHostName();
        return new InstrumentedPollContext(eventIpcManager, pollerConfig, queryManager,
                locationAwarePingClient, tsidFactory, localHostName,
                "OpenNMS.Poller.DefaultPollContext",
                meterRegistry, publisherProvider.getIfAvailable());
    }

    /**
     * In-memory tree of pollable nodes, interfaces, and services.
     */
    @Bean
    public PollableNetwork pollableNetwork(PollContext pollContext) {
        return new PollableNetwork(pollContext);
    }

    /**
     * The Poller daemon.
     *
     * <p>Constructor injection handles the 8 core dependencies. The remaining
     * three (pollerConfig, network, eventIpcManager) are set via setters because
     * they were kept as setter-injected fields during the constructor injection
     * migration (Task 2).</p>
     */
    @Bean
    public Poller poller(QueryManager queryManager,
                         MonitoredServiceDao monitoredServiceDao,
                         OutageDao outageDao,
                         TransactionTemplate transactionTemplate,
                         PersisterFactory persisterFactory,
                         ThresholdingService thresholdingService,
                         LocationAwarePollerClient locationAwarePollerClient,
                         ReadablePollOutagesDao pollOutagesDao,
                         PollerConfig pollerConfig,
                         PollContext pollContext,
                         PollableNetwork pollableNetwork,
                         EventIpcManager eventIpcManager) {
        var poller = new Poller(queryManager, monitoredServiceDao, outageDao,
                transactionTemplate, persisterFactory, thresholdingService,
                locationAwarePollerClient, pollOutagesDao);
        poller.setPollerConfig(pollerConfig);
        poller.setNetwork(pollableNetwork);
        poller.setEventIpcManager(eventIpcManager);
        return poller;
    }

    /**
     * Wraps the Poller daemon in a {@link SmartLifecycle} so Spring Boot
     * manages its startup and shutdown. Phase is {@code Integer.MAX_VALUE}
     * so the daemon starts last (after all infrastructure beans) and stops first.
     */
    @Bean
    public SmartLifecycle pollerLifecycle(Poller poller) {
        return new SpringServiceDaemonSmartLifecycle(poller);
    }

}
