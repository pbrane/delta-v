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
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.deltav.core.daemon.common.XmlConfigPostProcessor;
import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.tsid.TsidFactory;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.features.distributed.kvstore.json.noop.NoOpJsonStore;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.config.PollerConfig;
import org.opennms.netmgt.config.PollerConfigFactory;
import org.opennms.netmgt.config.SnmpPeerFactory;
import org.opennms.netmgt.config.poller.PollerConfiguration;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.snmp.SnmpConfig;
import org.opennms.netmgt.config.dao.outages.api.ReadablePollOutagesDao;
import org.opennms.netmgt.config.dao.outages.impl.OnmsPollOutagesDao;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.OutageDao;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.icmp.proxy.LocationAwarePingClient;
import org.opennms.netmgt.poller.LocationAwarePollerClient;
import org.opennms.netmgt.poller.Poller;
import org.opennms.netmgt.poller.QueryManager;
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
 * <p>The {@link PollerConfigFactory} is created with a constructor-injected
 * {@link FilterDao}, eliminating the hidden {@code FilterDaoFactory.getInstance()}
 * coupling and the need for {@code @DependsOn} bean ordering.</p>
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

    /**
     * Loads poller-configuration.xml via Jackson XmlMapper and creates a
     * PollerConfigFactory with constructor-injected FilterDao.
     *
     * <p>The FilterDao is used for filter rule validation during init and
     * for IP address resolution at runtime. This replaces the legacy
     * {@code PollerConfigFactory.init()} which used
     * {@code FilterDaoFactory.getInstance()} internally.</p>
     */
    @Bean
    public PollerConfig pollerConfig(FilterDao filterDao) throws IOException {
        var configFile = new java.io.File(opennmsHome, "etc/poller-configuration.xml");
        LOG.info("Loading PollerConfigFactory from {}", configFile);
        var config = XML_MAPPER.readValue(configFile, PollerConfiguration.class);
        patchNestedXmlParameters(configFile, config);
        PollerConfigFactory.validate(config, filterDao);
        var factory = new PollerConfigFactory(configFile.lastModified(), config, filterDao);
        PollerConfigFactory.setInstance(factory);
        return factory;
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
     * via {@link PollResultPublisher}.
     *
     * <p>The {@link ObjectProvider} for {@link PollResultPublisher} is used
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
                                  ObjectProvider<PollResultPublisher> publisherProvider) {
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

    /**
     * Patches parameters with nested XML content that Jackson XmlMapper
     * silently drops (e.g., {@code <page-sequence>} inside {@code <parameter>}).
     *
     * <p>Jackson's JaxbAnnotationModule does not support {@code @XmlAnyElement}
     * with {@code @XmlJavaTypeAdapter}, so nested XML is lost during
     * deserialization. This method re-reads the file with a DOM parser and
     * sets the nested XML as a string value on the affected parameters.
     * PageSequenceMonitor accepts both PageSequence objects and XML strings.</p>
     */
    private void patchNestedXmlParameters(File configFile, PollerConfiguration config) {
        Map<String, String> nestedParams = XmlConfigPostProcessor.extractNestedXmlParameters(configFile);
        if (nestedParams.isEmpty()) {
            return;
        }
        for (var pkg : config.getPackages()) {
            for (var service : pkg.getServices()) {
                for (var param : service.getParameters()) {
                    String lookupKey = pkg.getName() + ":" + service.getName() + ":" + param.getKey();
                    String xmlString = nestedParams.get(lookupKey);
                    if (xmlString != null && param.getValue() == null) {
                        param.setValue(xmlString);
                        LOG.debug("Patched parameter {}.{}.{} with nested XML",
                                pkg.getName(), service.getName(), param.getKey());
                    }
                }
            }
        }
    }
}
