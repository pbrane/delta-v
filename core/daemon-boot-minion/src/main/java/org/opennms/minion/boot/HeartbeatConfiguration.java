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

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.opennms.core.ipc.sink.api.SyncDispatcher;
import org.opennms.distributed.core.api.MinionIdentity;
import org.opennms.minion.heartbeat.common.HeartbeatModule;
import org.opennms.minion.heartbeat.common.MinionIdentityDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the heartbeat producer that sends periodic identity messages
 * to the core instance via the Sink API.
 *
 * <p>The original {@code HeartbeatProducer} uses OSGi's {@code FrameworkUtil.getBundle()}
 * to get the Minion version, which NPEs outside an OSGi container. This configuration
 * creates an inline heartbeat that uses Spring Boot's build properties for version info.</p>
 */
@Configuration
@ConditionalOnProperty(name = "opennms.minion.heartbeat.enabled", havingValue = "true", matchIfMissing = true)
public class HeartbeatConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatConfiguration.class);
    private static final int PERIOD_MS = 30_000;

    @Bean(destroyMethod = "cancel")
    public Timer heartbeatTimer(
            MinionIdentity identity,
            MessageDispatcherFactory dispatcherFactory,
            @Value("${spring.application.version:0.0.0}") String version) {

        MinionIdentityDTO identityDTO = new MinionIdentityDTO(identity);
        SyncDispatcher<MinionIdentityDTO> dispatcher =
                dispatcherFactory.createSyncDispatcher(new HeartbeatModule());

        Timer timer = new Timer("minion-heartbeat", true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    identityDTO.setVersion(version);
                    identityDTO.setTimestamp(new Date());
                    dispatcher.send(identityDTO);
                    LOG.info("Sent heartbeat for minion {} at {}", identity.getId(), identity.getLocation());
                } catch (Throwable t) {
                    LOG.error("Heartbeat failed, retrying in {}ms", PERIOD_MS, t);
                }
            }
        }, 0, PERIOD_MS);
        return timer;
    }
}
