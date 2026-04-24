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

import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;

import org.deltav.horizon.metrics.HorizonMetricsBridge;
import org.opennms.core.ipc.twin.common.LocalTwinSubscriberImpl;
import org.opennms.core.ipc.twin.kafka.publisher.KafkaTwinPublisher;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.distributed.core.api.Identity;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.passive.PassiveStatusKeeper;
import org.opennms.netmgt.passive.PassiveStatusTwinPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot configuration for passive status monitoring and Twin API publishing.
 *
 * <p>Wires the passive status subsystem so that Pollerd receives
 * {@code passiveServiceStatus} events via Kafka, {@link PassiveStatusKeeper}
 * tracks the state, and {@link PassiveStatusTwinPublisher} pushes updates
 * to Minion via {@link KafkaTwinPublisher}.</p>
 *
 * <p>Mirrors the bean definitions from the legacy Karaf XML context
 * {@code applicationContext-daemon-loader-pollerd.xml}.</p>
 */
@Configuration
public class PollerdPassiveStatusConfiguration {

    @Bean(initMethod = "init", destroyMethod = "stop")
    public PassiveStatusKeeper passiveStatusKeeper(
            EventIpcManager eventIpcManager,
            DataSource dataSource) {
        var keeper = new PassiveStatusKeeper();
        keeper.setEventManager(eventIpcManager);
        keeper.setDataSource(dataSource);
        PassiveStatusKeeper.setInstance(keeper);
        return keeper;
    }

    @Bean
    public Identity twinIdentity() {
        return new InlineIdentity();
    }

    @Bean
    public LocalTwinSubscriberImpl localTwinSubscriber(Identity twinIdentity) {
        return new LocalTwinSubscriberImpl(twinIdentity);
    }

    @Bean
    public MetricRegistry pollerdTwinMetricRegistry() {
        return new MetricRegistry();
    }

    @Bean
    public HorizonMetricsBridge pollerdTwinMetricsBridge(MetricRegistry pollerdTwinMetricRegistry) {
        return new HorizonMetricsBridge(pollerdTwinMetricRegistry, "opennms");
    }

    @Bean(initMethod = "init", destroyMethod = "close")
    public KafkaTwinPublisher kafkaTwinPublisher(
            LocalTwinSubscriberImpl localTwinSubscriber,
            TracerRegistry tracerRegistry,
            MetricRegistry pollerdTwinMetricRegistry) {
        return new KafkaTwinPublisher(localTwinSubscriber, tracerRegistry, pollerdTwinMetricRegistry);
    }

    @Bean(initMethod = "init", destroyMethod = "close")
    public PassiveStatusTwinPublisher passiveStatusTwinPublisher(
            KafkaTwinPublisher kafkaTwinPublisher,
            PassiveStatusKeeper passiveStatusKeeper,
            EventIpcManager eventIpcManager) {
        return new PassiveStatusTwinPublisher(kafkaTwinPublisher, passiveStatusKeeper, eventIpcManager);
    }
}
