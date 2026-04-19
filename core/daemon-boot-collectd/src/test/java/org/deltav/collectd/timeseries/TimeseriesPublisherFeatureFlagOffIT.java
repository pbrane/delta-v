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
package org.deltav.collectd.timeseries;

import static org.assertj.core.api.Assertions.assertThat;

import org.deltav.collectd.identity.AgentIdentityCapturingCollectorClient;
import org.deltav.collectd.identity.AgentIdentityHolder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = {
        TimeseriesPublisherFeatureFlagOffIT.TestApp.class
})
@TestPropertySource(properties = {
        "deltav.timeseries.enabled=false",
        "spring.main.web-application-type=none",
        "spring.main.banner-mode=off"
})
class TimeseriesPublisherFeatureFlagOffIT {

    /**
     * Minimal boot application that enables Spring Boot auto-configuration. With
     * deltav.timeseries.enabled=false, the @ConditionalOnProperty on
     * TimeseriesKafkaPublisherConfiguration skips all beans — including the
     * compositePersisterFactory which would otherwise require the Kafka binder.
     */
    @EnableAutoConfiguration
    @Import({
            TimeseriesKafkaPublisherConfiguration.class,
            TestChannelBinderConfiguration.class
    })
    static class TestApp {
    }

    @Autowired
    ApplicationContext ctx;

    @Test
    void publisherBeanIsAbsentWhenFlagOff() {
        assertThat(ctx.getBeanNamesForType(TimeseriesKafkaPublisher.class)).isEmpty();
    }

    @Test
    void persisterBeanIsAbsentWhenFlagOff() {
        assertThat(ctx.getBeanNamesForType(TimeseriesKafkaPersister.class)).isEmpty();
    }

    @Test
    void newTopicBeansAreAbsentWhenFlagOff() {
        assertThat(ctx.getBeanNamesForType(org.apache.kafka.clients.admin.NewTopic.class)).isEmpty();
    }

    @Test
    void agentIdentityCapturingClientBeanIsAbsentWhenFlagOff() {
        // Feature-flag off ⇒ decorator not registered ⇒ horizon's raw
        // LocationAwareCollectorClient wins. This test doesn't try to resolve
        // LocationAwareCollectorClient directly (the CollectdRpcConfiguration
        // bean isn't on this minimal test's classpath) — it just asserts the
        // decorator type is not instantiated.
        assertThat(ctx.getBeanNamesForType(AgentIdentityCapturingCollectorClient.class)).isEmpty();
    }

    @Test
    void agentIdentityHolderBeanIsAbsentWhenFlagOff() {
        assertThat(ctx.getBeanNamesForType(AgentIdentityHolder.class)).isEmpty();
    }
}
