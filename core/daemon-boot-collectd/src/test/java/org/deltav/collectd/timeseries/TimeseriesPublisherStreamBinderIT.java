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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;

import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectionSetVisitor;
import org.opennms.netmgt.collection.api.CollectionStatus;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = {
        TimeseriesPublisherStreamBinderIT.TestApp.class,
        TimeseriesPublisherStreamBinderIT.Stubs.class
})
@TestPropertySource(properties = {
        "deltav.timeseries.enabled=true",
        "spring.main.web-application-type=none",
        "spring.main.banner-mode=off"
})
class TimeseriesPublisherStreamBinderIT {

    /**
     * Minimal boot application that enables Spring Boot auto-configuration so that
     * SCS BindingServiceConfiguration (which creates StreamBridge) is triggered.
     * TestChannelBinderConfiguration provides the in-process test binder via
     * @ConditionalOnMissingBean(Binder.class) — the Kafka binder is loaded lazily
     * through the BinderFactory, not at startup, so no broker is needed.
     */
    @EnableAutoConfiguration
    @Import({
            TimeseriesKafkaPublisherConfiguration.class,
            TestChannelBinderConfiguration.class
    })
    static class TestApp {
    }

    @Autowired
    TimeseriesKafkaPublisher publisher;

    @Autowired
    OutputDestination output;

    @Test
    void publisherBeanIsPresentWhenFlagIsOn() {
        assertThat(publisher).isNotNull();
    }

    @Test
    void publishedMessageCarriesProtobufPayloadAndPartitionKey() throws Exception {
        CollectionSet set = buildMockSet(1700000000000L);

        publisher.publish(set, "default", 42, "Default");

        Message<byte[]> received = output.receive(2000, "deltav-timeseries");
        assertThat(received).isNotNull();
        TimeseriesBatch batch = TimeseriesBatch.parseFrom(received.getPayload());
        assertThat(batch.getNodeId()).isEqualTo(42);
        assertThat(batch.getLocation()).isEqualTo("Default");
        assertThat(received.getHeaders().get(KafkaHeaders.KEY)).isEqualTo("Default@42".getBytes());
    }

    private CollectionSet buildMockSet(long ts) {
        CollectionSet set = mock(CollectionSet.class);
        when(set.getStatus()).thenReturn(CollectionStatus.SUCCEEDED);
        when(set.getCollectionTimestamp()).thenReturn(new Date(ts));
        org.mockito.Mockito.doAnswer(inv -> {
            CollectionSetVisitor visitor = inv.getArgument(0);
            org.opennms.netmgt.collection.api.CollectionResource res =
                    mock(org.opennms.netmgt.collection.api.CollectionResource.class);
            when(res.getResourceTypeName()).thenReturn("node");
            when(res.getInstance()).thenReturn(null);
            when(res.getParent()).thenReturn(org.opennms.netmgt.model.ResourcePath.get("node[42]"));
            org.opennms.netmgt.collection.api.AttributeGroup g =
                    mock(org.opennms.netmgt.collection.api.AttributeGroup.class);
            when(g.getName()).thenReturn("mib2-system");
            org.opennms.netmgt.collection.api.CollectionAttribute a =
                    mock(org.opennms.netmgt.collection.api.CollectionAttribute.class);
            when(a.getName()).thenReturn("sysUpTime");
            when(a.getType()).thenReturn(org.opennms.netmgt.collection.api.AttributeType.GAUGE);
            when(a.getNumericValue()).thenReturn(12345);
            visitor.visitCollectionSet(set);
            visitor.visitResource(res);
            visitor.visitGroup(g);
            visitor.visitAttribute(a);
            visitor.completeAttribute(a);
            visitor.completeGroup(g);
            visitor.completeResource(res);
            visitor.completeCollectionSet(set);
            return null;
        }).when(set).visit(org.mockito.ArgumentMatchers.any());
        return set;
    }

    @TestConfiguration
    static class Stubs {
        /**
         * The composite PersisterFactory bean in TimeseriesKafkaPublisherConfiguration
         * needs a @Qualifier("timeseriesPersisterFactory") inner PersisterFactory. In
         * tests we don't have the real TimeseriesPersisterFactory on the classpath,
         * so provide a mock stub.
         */
        @Bean(name = "timeseriesPersisterFactory")
        PersisterFactory stubInnerFactory() {
            return mock(PersisterFactory.class);
        }

        /**
         * The @Primary decorator bean in TimeseriesKafkaPublisherConfiguration
         * requires a @Qualifier("locationAwareCollectorClient") delegate. The real
         * bean is registered by CollectdRpcConfiguration which is not on this
         * minimal test classpath, so provide a stub.
         */
        @Bean(name = "locationAwareCollectorClient")
        org.opennms.netmgt.collection.api.LocationAwareCollectorClient stubCollectorClient() {
            return mock(org.opennms.netmgt.collection.api.LocationAwareCollectorClient.class);
        }
    }
}
