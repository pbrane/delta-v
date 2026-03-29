/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.minion.boot;

import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.syslogd.SyslogConfigBean;
import org.opennms.netmgt.syslogd.SyslogReceiverJavaNetImpl;
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
                                                     MessageDispatcherFactory messageDispatcherFactory,
                                                     DistPollerDao distPollerDao) {
        SyslogReceiverJavaNetImpl receiver = new SyslogReceiverJavaNetImpl(config);
        receiver.setMessageDispatcherFactory(messageDispatcherFactory);
        receiver.setDistPollerDao(distPollerDao);
        return receiver;
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
