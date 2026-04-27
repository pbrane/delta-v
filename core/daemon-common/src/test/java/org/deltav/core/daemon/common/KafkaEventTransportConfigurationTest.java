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
package org.deltav.core.daemon.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Verifies that {@link KafkaEventTransportConfiguration} is a valid Spring
 * {@link Configuration} class with the expected bean definitions.
 *
 * <p>This test does not start Kafka or a full Spring context. Integration
 * testing with a real Kafka broker is covered by the Alarmd integration
 * test (Task 12) using Testcontainers.</p>
 */
class KafkaEventTransportConfigurationTest {

    @Test
    void configurationClassHasSpringAnnotation() {
        assertThat(KafkaEventTransportConfiguration.class)
                .hasAnnotation(Configuration.class);
    }

    @Test
    void kafkaEventForwarderBeanMethodExists() throws NoSuchMethodException {
        var method = KafkaEventTransportConfiguration.class
                .getDeclaredMethod("kafkaEventForwarder");
        assertThat(method.isAnnotationPresent(Bean.class)).isTrue();
    }

    @Test
    void kafkaEventSubscriptionServiceBeanMethodExists() throws NoSuchMethodException {
        var method = KafkaEventTransportConfiguration.class
                .getDeclaredMethod("kafkaEventSubscriptionService");
        assertThat(method.isAnnotationPresent(Bean.class)).isTrue();

        Bean beanAnnotation = method.getAnnotation(Bean.class);
        assertThat(beanAnnotation.destroyMethod()).contains("stop");
    }

    @Test
    void eventIpcManagerBeanMethodExists() throws NoSuchMethodException {
        var method = KafkaEventTransportConfiguration.class
                .getDeclaredMethod("eventIpcManager",
                        org.deltav.core.event.forwarder.kafka.KafkaEventForwarder.class,
                        org.deltav.core.event.forwarder.kafka.KafkaEventSubscriptionService.class,
                        EventConfEnrichmentService.class);
        assertThat(method.isAnnotationPresent(Bean.class)).isTrue();
    }
}
