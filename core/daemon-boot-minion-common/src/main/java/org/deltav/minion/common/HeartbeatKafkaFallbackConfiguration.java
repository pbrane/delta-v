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

import org.opennms.core.ipc.sink.api.MessageDispatcherFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@code MINION_TRANSPORT=kafka} rollback path: aliases the Kafka Syslog
 * {@link MessageDispatcherFactory} under the {@code heartbeatDispatcherFactory}
 * bean name so HeartbeatConfiguration's {@code @Qualifier} injection still
 * resolves regardless of selected transport.
 *
 * <p>After PR3's per-sink split (Task 4) the rc1 monolithic
 * {@code KafkaSinkClientConfiguration} no longer exists. Heartbeat does not
 * have its own Kafka {@link MessageDispatcherFactory}, so the fallback now
 * borrows the syslog one. The factory class itself is the same regardless of
 * which sink it serves; any qualified Kafka factory bean would do here.
 *
 * <p><b>Operational coupling:</b> rolling back to {@code MINION_TRANSPORT=kafka}
 * also requires {@code MINION_SINK_SYSLOG_TRANSPORT=kafka} so the
 * {@code syslogDispatcherFactory} bean exists. This is operationally uncommon
 * (mixing transport flags) and may be cleaned up post-rc2 by giving heartbeat
 * its own dedicated Kafka @Configuration.
 */
@Configuration
@ConditionalOnExpression(
        "'${opennms.minion.transport:grpc}' == 'kafka' "
        + "&& '${opennms.minion.transport.sink.syslog:grpc}' == 'kafka'")
public class HeartbeatKafkaFallbackConfiguration {

    @Bean(name = "heartbeatDispatcherFactory")
    public MessageDispatcherFactory heartbeatDispatcherFactory(
            @Qualifier("syslogDispatcherFactory") MessageDispatcherFactory kafkaSyslog) {
        return kafkaSyslog;
    }
}
