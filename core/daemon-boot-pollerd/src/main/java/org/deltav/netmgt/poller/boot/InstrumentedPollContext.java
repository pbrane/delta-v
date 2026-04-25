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
package org.deltav.netmgt.poller.boot;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.opennms.core.tsid.TsidFactory;
import org.opennms.netmgt.config.PollerConfig;
import org.opennms.netmgt.events.api.EventIpcManager;
import org.opennms.netmgt.icmp.proxy.LocationAwarePingClient;
import org.opennms.netmgt.poller.PollStatus;
import org.opennms.netmgt.poller.QueryManager;
import org.opennms.netmgt.poller.pollables.PollEvent;
import org.opennms.netmgt.poller.pollables.PollableService;

/**
 * {@link StandalonePollContext} subclass that increments {@code deltav_pollerd_*}
 * Micrometer counters on every poll completion and outage transition, and
 * optionally publishes per-poll response-time samples to the
 * {@code deltav-timeseries} Kafka topic via {@link PollResultPublisher}.
 *
 * <p>The publisher is wired only when {@code deltav.timeseries.enabled=true}
 * in {@code application.yml}; otherwise it is null and Phase 3 publishing
 * is a no-op (Phase 2 metrics still fire). This matches the collectd module's
 * conditional pattern.
 */
public class InstrumentedPollContext extends StandalonePollContext {

    private final MeterRegistry registry;
    private final PollResultPublisher publisher;
    private final Timer pollDuration;

    public InstrumentedPollContext(EventIpcManager eventManager, PollerConfig pollerConfig,
                                   QueryManager queryManager, LocationAwarePingClient pingClient,
                                   TsidFactory tsidFactory, String localHostName, String name,
                                   MeterRegistry registry, PollResultPublisher publisher) {
        super(eventManager, pollerConfig, queryManager, pingClient, tsidFactory, localHostName, name);
        this.registry = registry;
        this.publisher = publisher;
        this.pollDuration = registry.timer(PollerdDomainMetrics.POLL_DURATION);
    }

    @Override
    public void trackPoll(PollableService service, PollStatus status) {
        super.trackPoll(service, status);
        recordPoll(service, status);
        if (publisher != null) {
            publisher.publish(service, status);
        }
    }

    @Override
    public void openOutage(PollableService service, PollEvent event) {
        super.openOutage(service, event);
        registry.counter(PollerdDomainMetrics.OUTAGES_OPENED,
                PollerdDomainMetrics.TAG_LOCATION, location(service)).increment();
    }

    @Override
    public void resolveOutage(PollableService service, PollEvent event) {
        super.resolveOutage(service, event);
        registry.counter(PollerdDomainMetrics.OUTAGES_RESOLVED,
                PollerdDomainMetrics.TAG_LOCATION, location(service)).increment();
    }

    private void recordPoll(PollableService service, PollStatus status) {
        String location = location(service);
        String result = status != null && status.getStatusName() != null
                ? status.getStatusName().toLowerCase() : "unknown";
        registry.counter(PollerdDomainMetrics.POLLS_COMPLETED,
                PollerdDomainMetrics.TAG_LOCATION, location,
                PollerdDomainMetrics.TAG_RESULT, result).increment();
        if (status != null && status.getResponseTime() != null) {
            pollDuration.record((long) (status.getResponseTime() * 1_000_000.0),
                    java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private static String location(PollableService service) {
        if (service == null) return "unknown";
        String l = service.getNodeLocation();
        return l != null ? l : "Default";
    }
}
