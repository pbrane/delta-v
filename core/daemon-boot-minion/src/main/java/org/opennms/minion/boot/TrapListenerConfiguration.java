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
import org.opennms.core.ipc.twin.api.TwinSubscriber;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.trapd.TrapdConfigBean;
import org.opennms.netmgt.trapd.TrapListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the SNMP trap listener and its Sink producer.
 *
 * <p>When enabled, opens UDP port 1162 (configurable) to receive SNMP traps
 * and forwards them to the core instance via the Sink API. Port binding
 * is inside this conditional -- when disabled, port 1162 is never opened.</p>
 *
 * <p>Lifecycle phase 400: listeners start last, after RPC server (300)
 * and Sink client (200) are ready.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.traps.enabled", havingValue = "true", matchIfMissing = true)
public class TrapListenerConfiguration {

    @Value("${opennms.minion.traps.port:1162}")
    private int trapPort;

    @Value("${opennms.minion.traps.address:*}")
    private String trapAddress;

    @Bean
    public TrapdConfigBean trapdConfigBean() {
        TrapdConfigBean config = new TrapdConfigBean();
        config.setSnmpTrapPort(trapPort);
        config.setSnmpTrapAddress(trapAddress);
        config.setNewSuspectOnTrap(false);
        config.setBatchSize(100);
        config.setBatchIntervalMs(500);
        config.setQueueSize(10000);
        return config;
    }

    @Bean
    public TrapListener trapListener(TrapdConfigBean config,
                                     MessageDispatcherFactory messageDispatcherFactory,
                                     DistPollerDao distPollerDao) throws Exception {
        TrapListener listener = new TrapListener(config);
        listener.setMessageDispatcherFactory(messageDispatcherFactory);
        listener.setDistPollerDao(distPollerDao);
        return listener;
    }

    /**
     * SmartLifecycle that activates the TrapListener at phase 400.
     *
     * <p>On start, calls {@code bind(TwinSubscriber)} which sets the subscriber
     * and immediately subscribes for configuration updates. When the core sends
     * a TrapListenerConfig via Twin, the listener opens the trap port. This
     * replaces the OSGi dynamic bind/unbind pattern.</p>
     *
     * <p>We do NOT call {@code TrapListener.start()} here because {@code bind()}
     * already subscribes (calling {@code start()} would double-subscribe).</p>
     */
    @Bean
    public SmartLifecycle trapListenerLifecycle(TrapListener trapListener, TwinSubscriber twinSubscriber) {
        return new SmartLifecycle() {
            private volatile boolean running;

            @Override
            public void start() {
                trapListener.bind(twinSubscriber);
                running = true;
            }

            @Override
            public void stop() {
                trapListener.stop();
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
