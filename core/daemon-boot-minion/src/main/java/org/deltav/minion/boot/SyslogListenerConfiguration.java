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

import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.syslogd.SyslogConfigBean;
import org.opennms.netmgt.syslogd.SyslogReceiverJavaNetImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the syslog listener using the pure-Java DatagramSocket implementation.
 *
 * <p><strong>CRITICAL:</strong> Uses {@link SyslogReceiverJavaNetImpl}, NOT
 * {@code SyslogReceiverCamelNettyImpl}. Camel 2.x has been excluded from
 * the POM (incompatible with Spring 7 / Spring Boot 4).</p>
 *
 * <p>When enabled, opens UDP port 1514 (configurable) to receive syslog
 * messages and forwards them to the core instance via the Sink API. Port
 * binding is inside this conditional -- when disabled, port 1514 is never
 * opened.</p>
 *
 * <p>Lifecycle phase 400: listeners start last, after RPC server (300)
 * and Sink client (200) are ready. The receiver's {@code run()} method
 * blocks, so it is started in a dedicated daemon thread.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.syslog.enabled", havingValue = "true", matchIfMissing = true)
public class SyslogListenerConfiguration {

    @Value("${opennms.minion.syslog.port:1514}")
    private int syslogPort;

    @Value("${opennms.minion.syslog.address:#{null}}")
    private String listenAddress;

    @Bean
    public SyslogConfigBean syslogConfigBean() {
        SyslogConfigBean config = new SyslogConfigBean();
        config.setSyslogPort(syslogPort);
        config.setListenAddress(listenAddress);
        config.setNewSuspectOnMessage(false);
        config.setBatchSize(100);
        config.setBatchIntervalMs(500);
        config.setQueueSize(10000);
        return config;
    }

    @Bean
    public SyslogReceiverJavaNetImpl syslogReceiver(SyslogConfigBean config,
                                                     @Qualifier("syslogDispatcherFactory")
                                                     MessageDispatcherFactory messageDispatcherFactory,
                                                     DistPollerDao distPollerDao) {
        return new SyslogReceiverJavaNetImpl(config, distPollerDao, messageDispatcherFactory);
    }

    @Bean
    public SmartLifecycle syslogListenerLifecycle(SyslogReceiverJavaNetImpl syslogReceiver) {
        return new SmartLifecycle() {
            private volatile boolean running;
            private Thread receiverThread;

            @Override
            public void start() {
                receiverThread = new Thread(syslogReceiver, "SyslogReceiver");
                receiverThread.setDaemon(true);
                receiverThread.start();
                running = true;
            }

            @Override
            public void stop() {
                try {
                    syslogReceiver.stop();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                running = false;
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
