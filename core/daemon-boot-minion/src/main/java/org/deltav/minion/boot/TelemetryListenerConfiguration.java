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
package org.deltav.minion.boot;

import java.util.EnumMap;
import java.util.Map;

import org.deltav.minion.telemetry.FlowProtocol;
import org.deltav.minion.telemetry.FlowSinkModule;
import org.deltav.minion.telemetry.FlowTelemetryMessage;
import org.deltav.minion.telemetry.FlowUdpListener;
import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the flow telemetry UDP listener and its four per-protocol Sink
 * dispatchers.
 *
 * <p>When enabled, opens UDP port {@code opennms.minion.telemetry.port}
 * (default 4729) to receive Netflow v5/v9, IPFIX, and sFlow datagrams
 * and forwards each one to the appropriate
 * {@code OpenNMS.Sink.Telemetry-*} Kafka topic via the Sink API.</p>
 *
 * <p>Lifecycle phase 400: listeners start last, after Sink client
 * (200) and RPC server (300) are ready. Matches the Trap and Syslog
 * listener pattern.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.telemetry.enabled", havingValue = "true", matchIfMissing = true)
public class TelemetryListenerConfiguration {

    @Value("${opennms.minion.telemetry.port:4729}")
    private int telemetryPort;

    @Value("${opennms.minion.telemetry.address:*}")
    private String bindAddress;

    @Value("${opennms.minion.telemetry.queue-size:10000}")
    private int queueSize;

    @Value("${opennms.minion.telemetry.num-threads:2}")
    private int numThreads;

    @Bean
    public FlowUdpListener flowUdpListener(@Qualifier("telemetryDispatcherFactory")
                                           MessageDispatcherFactory messageDispatcherFactory,
                                           DistPollerDao distPollerDao) {
        Map<FlowProtocol, AsyncDispatcher<FlowTelemetryMessage>> dispatchers =
                new EnumMap<>(FlowProtocol.class);
        for (FlowProtocol protocol : FlowProtocol.values()) {
            FlowSinkModule module = new FlowSinkModule(protocol, queueSize, numThreads);
            dispatchers.put(protocol, messageDispatcherFactory.createAsyncDispatcher(module));
        }

        var self = distPollerDao.whoami();
        return new FlowUdpListener(
                telemetryPort,
                bindAddress,
                dispatchers,
                self.getLocation(),
                self.getId());
    }

    @Bean
    public SmartLifecycle flowUdpListenerLifecycle(FlowUdpListener listener) {
        return new SmartLifecycle() {
            private volatile boolean running;

            @Override
            public void start() {
                try {
                    listener.start();
                    running = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while starting FlowUdpListener", e);
                }
            }

            @Override
            public void stop() {
                try {
                    listener.stop();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    running = false;
                }
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 400;
            }
        };
    }
}
