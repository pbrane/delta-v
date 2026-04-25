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
package org.deltav.netmgt.discovery.boot;

import java.util.concurrent.CompletableFuture;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.opennms.netmgt.config.discovery.DiscoveryConfiguration;
import org.opennms.netmgt.discovery.DiscoveryTaskExecutorImpl;
import org.opennms.netmgt.discovery.RangeChunker;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.icmp.proxy.LocationAwarePingClient;
import org.opennms.netmgt.provision.LocationAwareDetectorClient;

/**
 * {@link DiscoveryTaskExecutorImpl} subclass that increments a counter and
 * times each {@code handleDiscoveryTask} invocation. Delegates to
 * {@code super.handleDiscoveryTask} so discovery logic is unchanged.
 *
 * <p>The result is a {@code CompletableFuture<Void>}; the timer records
 * the asynchronous duration via {@code whenComplete}, not the synchronous
 * call duration (which is just the chunk-and-dispatch latency).
 */
public class CountingDiscoveryTaskExecutor extends DiscoveryTaskExecutorImpl {

    private final Counter tasksHandled;
    private final Timer handleDuration;

    public CountingDiscoveryTaskExecutor(RangeChunker rangeChunker,
                                         LocationAwarePingClient pingClient,
                                         EventForwarder eventForwarder,
                                         LocationAwareDetectorClient detectorClient,
                                         MeterRegistry registry) {
        super(rangeChunker, pingClient, eventForwarder, detectorClient);
        this.tasksHandled = registry.counter(DiscoveryDomainMetrics.TASKS_HANDLED);
        this.handleDuration = registry.timer(DiscoveryDomainMetrics.HANDLE_DURATION);
    }

    @Override
    public CompletableFuture<Void> handleDiscoveryTask(DiscoveryConfiguration config) {
        tasksHandled.increment();
        Timer.Sample sample = Timer.start();
        CompletableFuture<Void> future = super.handleDiscoveryTask(config);
        future.whenComplete((v, t) -> sample.stop(handleDuration));
        return future;
    }
}
