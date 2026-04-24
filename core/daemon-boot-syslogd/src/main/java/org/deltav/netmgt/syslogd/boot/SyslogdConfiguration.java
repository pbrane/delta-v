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
package org.deltav.netmgt.syslogd.boot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import org.deltav.core.daemon.common.JdbcDistPollerDao;
import org.deltav.core.daemon.common.JdbcInterfaceToNodeCache;
import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.config.syslogd.SyslogdConfigurationGroup;
import org.opennms.netmgt.config.syslogd.HideMatch;
import org.opennms.netmgt.config.syslogd.UeiMatch;
import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.dao.api.InterfaceToNodeCache;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.provision.LocationAwareDnsLookupClient;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot @Configuration that wires all Syslogd beans.
 *
 * <p>Replaces the Karaf-era {@code applicationContext-daemon-loader-syslogd.xml}.</p>
 *
 * <p>{@code SyslogSinkConsumer} implements {@code InitializingBean} -- Spring
 * calls {@code afterPropertiesSet()} natively after constructor injection.
 * No {@code initMethod} workaround needed (unlike Trapd's {@code javax.annotation.PostConstruct}).</p>
 */
@Configuration
public class SyslogdConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(SyslogdConfiguration.class);

    private static final XmlMapper XML_MAPPER;
    static {
        XML_MAPPER = XmlMapper.builder()
                .defaultUseWrapper(false)
                .build();
        XML_MAPPER.registerModule(new JaxbAnnotationModule());
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Value("${opennms.syslogd.dnscache.config:maximumSize=1000,expireAfterWrite=8h}")
    private String dnsCacheConfig;

    @Bean
    public SyslogdConfig syslogdConfig(@Value("${opennms.home}") String opennmsHome) throws IOException {
        var configFile = new File(opennmsHome, "etc/syslogd-configuration.xml");
        if (!configFile.exists()) {
            throw new IOException("syslogd-configuration.xml not found at " + configFile);
        }
        LOG.info("Loading syslogd configuration from {}", configFile);
        var xmlConfig = XML_MAPPER.readValue(configFile,
                org.opennms.netmgt.config.syslogd.SyslogdConfiguration.class);

        // Process <import-file> directives — merge UEI matches and hide matches
        // from included files into the main configuration.
        var configDir = configFile.getParentFile();
        for (String fileName : xmlConfig.getImportFiles()) {
            var includeFile = new File(configDir, fileName);
            if (!includeFile.exists()) {
                LOG.warn("Import file {} not found, skipping", includeFile);
                continue;
            }
            LOG.info("Loading syslogd import file {}", includeFile);
            var includeCfg = XML_MAPPER.readValue(includeFile, SyslogdConfigurationGroup.class);
            if (includeCfg.getUeiMatches() != null) {
                if (xmlConfig.getUeiMatches() == null) {
                    xmlConfig.setUeiMatches(new ArrayList<>());
                }
                for (UeiMatch ueiMatch : includeCfg.getUeiMatches()) {
                    xmlConfig.addUeiMatch(ueiMatch);
                }
            }
            if (includeCfg.getHideMatches() != null) {
                if (xmlConfig.getHideMatches() == null) {
                    xmlConfig.setHideMatches(new ArrayList<>());
                }
                for (HideMatch hideMatch : includeCfg.getHideMatches()) {
                    xmlConfig.addHideMatch(hideMatch);
                }
            }
        }

        return new SyslogdConfigAdapter(xmlConfig);
    }

    @Bean
    public MetricRegistry syslogdMetricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge syslogdMetricsBridge(MetricRegistry syslogdMetricRegistry) {
        return new HorizonMetricsBridge(syslogdMetricRegistry, "opennms");
    }

    @Bean
    public DistPollerDao distPollerDao(DataSource dataSource) {
        return new JdbcDistPollerDao(dataSource);
    }

    @Bean
    public InterfaceToNodeCache interfaceToNodeCache(DataSource dataSource) {
        // AbstractInterfaceToNodeCache.setInstance() is called inside refresh(),
        // which fires immediately via @Scheduled(initialDelayString = "0").
        // This ensures the singleton is set after data is loaded, not before.
        return new JdbcInterfaceToNodeCache(dataSource);
    }

    @Bean
    public LocationAwareDnsLookupClient locationAwareDnsLookupClient() {
        return new LocalDnsLookupClient();
    }

    @Bean
    public SyslogSinkConsumer syslogSinkConsumer(MetricRegistry metricRegistry,
                                                  MessageConsumerManager messageConsumerManager,
                                                  SyslogdConfig syslogdConfig,
                                                  DistPollerDao distPollerDao,
                                                  EventForwarder eventForwarder,
                                                  LocationAwareDnsLookupClient locationAwareDnsLookupClient) {
        // Bridge DNS cache config for SyslogSinkConsumer constructor
        System.setProperty("org.opennms.netmgt.syslogd.dnscache.config", dnsCacheConfig);

        // SyslogSinkConsumer implements InitializingBean -- Spring calls
        // afterPropertiesSet() after construction completes.
        // afterPropertiesSet() registers consumer with MessageConsumerManager,
        // which triggers KafkaSinkBridge.setModule(), starting Kafka polling.
        //
        // Note: SyslogSinkConsumer.getModule() internally creates its own
        // SyslogSinkModule using its syslogdConfig and distPollerDao.
        // No separate SyslogSinkModule @Bean is needed.
        return new SyslogSinkConsumer(metricRegistry, messageConsumerManager, syslogdConfig,
                distPollerDao, eventForwarder, locationAwareDnsLookupClient);
    }
}
