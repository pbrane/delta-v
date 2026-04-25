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
package org.deltav.netmgt.syslogd.boot;

import java.util.List;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.opennms.core.ipc.sink.api.MessageConsumerManager;
import org.opennms.netmgt.config.SyslogdConfig;
import org.opennms.netmgt.dao.api.DistPollerDao;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.provision.LocationAwareDnsLookupClient;
import org.opennms.netmgt.syslogd.SyslogSinkConsumer;
import org.opennms.netmgt.syslogd.api.SyslogMessageDTO;
import org.opennms.netmgt.syslogd.api.SyslogMessageLogDTO;

/**
 * {@link SyslogSinkConsumer} subclass that increments Micrometer counters and
 * records timing on every Sink batch arriving from a Minion. Delegates to
 * {@code super.handleMessage} so syslog processing semantics are unchanged.
 *
 * <p>Horizon's internal Dropwizard timers (consumer/toEvent/broadcast) are
 * already bridged to {@code opennms_*} meters via {@code HorizonMetricsBridge};
 * this subclass adds the delta-v-native batch-shape signals on top.
 */
public class CountingSyslogSinkConsumer extends SyslogSinkConsumer {

    private final MeterRegistry registry;
    private final Timer handleDuration;

    public CountingSyslogSinkConsumer(MetricRegistry metricRegistry,
                                      MessageConsumerManager messageConsumerManager,
                                      SyslogdConfig syslogdConfig,
                                      DistPollerDao distPollerDao,
                                      EventForwarder eventForwarder,
                                      LocationAwareDnsLookupClient dnsLookupClient,
                                      MeterRegistry registry) {
        super(metricRegistry, messageConsumerManager, syslogdConfig, distPollerDao,
                eventForwarder, dnsLookupClient);
        this.registry = registry;
        this.handleDuration = registry.timer(SyslogdDomainMetrics.HANDLE_DURATION);
    }

    @Override
    public void handleMessage(SyslogMessageLogDTO message) {
        String location = location(message);
        registry.counter(SyslogdDomainMetrics.MESSAGE_LOGS_RECEIVED,
                SyslogdDomainMetrics.TAG_LOCATION, location).increment();
        List<SyslogMessageDTO> batch = message != null ? message.getMessages() : null;
        int size = batch != null ? batch.size() : 0;
        if (size > 0) {
            registry.counter(SyslogdDomainMetrics.MESSAGES_IN_BATCH,
                    SyslogdDomainMetrics.TAG_LOCATION, location).increment(size);
        }
        handleDuration.record(() -> super.handleMessage(message));
    }

    private static String location(SyslogMessageLogDTO message) {
        if (message == null) return "unknown";
        String l = message.getLocation();
        return l != null ? l : "unknown";
    }
}
