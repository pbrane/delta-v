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
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import org.opennms.core.mate.api.EntityScopeProvider;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.netmgt.collection.api.CollectionAgentFactory;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.config.PollerConfig;
import org.opennms.netmgt.config.PollerConfigFactory;
import org.opennms.netmgt.config.SnmpPeerFactory;
import org.opennms.netmgt.config.poller.PollerConfiguration;
import org.opennms.netmgt.filter.api.FilterDao;
import org.opennms.netmgt.config.api.SnmpAgentConfigFactory;
import org.opennms.netmgt.config.snmp.SnmpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opennms.netmgt.dao.api.ApplicationDao;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.MonitoringLocationDao;
import org.opennms.netmgt.dao.api.OutageDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.events.api.AnnotationBasedEventListenerAdapter;
import org.opennms.netmgt.perspectivepoller.PerspectivePollerd;
import org.opennms.netmgt.perspectivepoller.PerspectiveServiceTracker;
import org.opennms.netmgt.poller.LocationAwarePollerClient;
import org.opennms.netmgt.threshd.api.ThresholdingService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.deltav.core.daemon.common.SpringServiceDaemonSmartLifecycle;
import org.deltav.poller.timeseries.ResponseTimePublisher;
import org.deltav.core.daemon.common.XmlConfigPostProcessor;
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
 * locations to detect location-specific outages. It shares poller-configuration.xml
 * with Pollerd but uses its own scheduling and outage tracking logic.</p>
 *
 * <p>The {@link PollerConfigFactory} is created with a constructor-injected
 * {@link FilterDao}, eliminating the hidden {@code FilterDaoFactory.getInstance()}
 * coupling and the need for {@code @DependsOn} bean ordering.</p>
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
     * Loads poller-configuration.xml via Jackson XmlMapper and creates a
     * PollerConfigFactory with constructor-injected FilterDao.
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
     * Patches parameters with nested XML content that Jackson XmlMapper
     * silently drops (e.g., {@code <page-sequence>} inside {@code <parameter>}).
     *
     * @see PollerdDaemonConfiguration for the full explanation
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
