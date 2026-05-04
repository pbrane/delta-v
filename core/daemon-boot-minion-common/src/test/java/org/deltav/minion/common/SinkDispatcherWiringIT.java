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
package org.deltav.minion.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.distributed.core.api.MinionIdentity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Wiring integration test verifying the per-sink Kafka {@link MessageDispatcherFactory}
 * beans load independently and resolve under their qualified names.
 *
 * <p>Each Kafka sink @Configuration is gated by its own
 * {@code opennms.minion.transport.sink.<type>=kafka} property; this test forces
 * all three on so we can assert all three qualified beans coexist.
 */
@SpringBootTest(classes = {
        MinionSinkMetricsConfiguration.class,
        KafkaSyslogDispatcherConfiguration.class,
        KafkaTrapDispatcherConfiguration.class,
        KafkaTelemetryDispatcherConfiguration.class,
})
@TestPropertySource(properties = {
        "opennms.minion.transport.sink.syslog=kafka",
        "opennms.minion.transport.sink.trap=kafka",
        "opennms.minion.transport.sink.telemetry=kafka",
        "opennms.kafka.bootstrap-servers=localhost:9092",
})
class SinkDispatcherWiringIT {

    @MockitoBean
    MinionIdentity minionIdentity;

    @MockitoBean
    TracerRegistry tracerRegistry;

    @Autowired
    @Qualifier("syslogDispatcherFactory")
    MessageDispatcherFactory syslogFactory;

    @Autowired
    @Qualifier("trapDispatcherFactory")
    MessageDispatcherFactory trapFactory;

    @Autowired
    @Qualifier("telemetryDispatcherFactory")
    MessageDispatcherFactory telemetryFactory;

    @Test
    void allThreeQualifiedFactories_loadIndependently() {
        assertThat(syslogFactory).isNotNull();
        assertThat(trapFactory).isNotNull();
        assertThat(telemetryFactory).isNotNull();
        assertThat(syslogFactory).isNotSameAs(trapFactory);
        assertThat(trapFactory).isNotSameAs(telemetryFactory);
    }
}
