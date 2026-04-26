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
import org.springframework.beans.factory.annotation.Qualifier;
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
            @Qualifier("heartbeatDispatcherFactory") MessageDispatcherFactory dispatcherFactory,
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
