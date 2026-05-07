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
package org.deltav.netmgt.perspectivepoller.boot;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.opennms.core.tracing.api.TracerRegistry;
import org.opennms.netmgt.collection.api.CollectionAgentFactory;
import org.opennms.netmgt.collection.api.PersisterFactory;
import org.opennms.netmgt.config.PollerConfig;
import org.opennms.netmgt.dao.api.ApplicationDao;
import org.opennms.netmgt.dao.api.MonitoredServiceDao;
import org.opennms.netmgt.dao.api.MonitoringLocationDao;
import org.opennms.netmgt.dao.api.OutageDao;
import org.opennms.netmgt.dao.api.SessionUtils;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.perspectivepoller.PerspectivePolledService;
import org.opennms.netmgt.perspectivepoller.PerspectivePollerd;
import org.opennms.netmgt.perspectivepoller.PerspectiveServiceTracker;
import org.opennms.netmgt.poller.LocationAwarePollerClient;
import org.opennms.netmgt.poller.PollStatus;
import org.opennms.netmgt.threshd.api.ThresholdingService;

/**
 * Subclass of horizon's {@link PerspectivePollerd} that adds delta-v
 * instrumentation by overriding {@code persistResponseTimeData}, the only
 * public per-poll callback exposed by horizon's PerspectivePollerd.
 *
 * <p>Per call, in order:
 * <ol>
 *   <li>Delegates to {@code super.persistResponseTimeData(...)} — preserves
 *       horizon's RRD-style persistence (no-op when the perspective service
 *       has no {@code RrdRepository} configured).</li>
 *   <li>Increments {@code deltav_perspective_polls_completed_total} tagged
 *       by {@code perspective}, {@code location}, and {@code result}.</li>
 *   <li>Records {@code deltav_perspective_poll_duration_seconds} from the
 *       poll's measured response time.</li>
 *   <li>If {@link PerspectiveResponseTimePublisher} is wired (Phase 3
 *       enabled), forwards the sample to the {@code deltav-timeseries}
 *       Kafka topic.</li>
 * </ol>
 *
 * <p>Per {@code feedback_rpc_timeout_no_outages}, this seam fires only on
 * RPC-successful polls — the horizon PollJob's exception branch never
 * reaches {@code persistResponseTimeData}, so RPC infrastructure failures
 * are correctly excluded from poll counters.
 */
public class InstrumentedPerspectivePollerd extends PerspectivePollerd {

    private final MeterRegistry meterRegistry;
    private final PerspectiveResponseTimePublisher publisher;
    private final Timer pollDuration;

    public InstrumentedPerspectivePollerd(SessionUtils sessionUtils,
                                          MonitoringLocationDao monitoringLocationDao,
                                          PollerConfig pollerConfig,
                                          MonitoredServiceDao monitoredServiceDao,
                                          LocationAwarePollerClient locationAwarePollerClient,
                                          ApplicationDao applicationDao,
                                          CollectionAgentFactory collectionAgentFactory,
                                          PersisterFactory persisterFactory,
                                          EventForwarder eventForwarder,
                                          ThresholdingService thresholdingService,
                                          OutageDao outageDao,
                                          TracerRegistry tracerRegistry,
                                          PerspectiveServiceTracker tracker,
                                          MeterRegistry meterRegistry,
                                          PerspectiveResponseTimePublisher publisher) {
        super(sessionUtils, monitoringLocationDao, pollerConfig, monitoredServiceDao,
                locationAwarePollerClient, applicationDao, collectionAgentFactory,
                persisterFactory, eventForwarder, thresholdingService, outageDao,
                tracerRegistry, tracker);
        this.meterRegistry = meterRegistry;
        this.publisher = publisher;
        this.pollDuration = meterRegistry.timer(PerspectivePollerdDomainMetrics.POLL_DURATION);
    }

    @Override
    public void persistResponseTimeData(PerspectivePolledService polledService, PollStatus pollStatus) {
        try {
            super.persistResponseTimeData(polledService, pollStatus);
        } finally {
            recordPoll(polledService, pollStatus);
            if (publisher != null) {
                publisher.publish(polledService, pollStatus);
            }
        }
    }

    private void recordPoll(PerspectivePolledService polledService, PollStatus status) {
        if (polledService == null) {
            return;
        }
        String perspective = polledService.getPerspectiveLocation() != null
                ? polledService.getPerspectiveLocation() : "Default";
        String residentLocation = polledService.getResidentLocation() != null
                ? polledService.getResidentLocation() : "Default";
        String result = status != null && status.getStatusName() != null
                ? status.getStatusName().toLowerCase() : "unknown";
        meterRegistry.counter(PerspectivePollerdDomainMetrics.POLLS_COMPLETED,
                PerspectivePollerdDomainMetrics.TAG_PERSPECTIVE, perspective,
                PerspectivePollerdDomainMetrics.TAG_LOCATION, residentLocation,
                PerspectivePollerdDomainMetrics.TAG_RESULT, result).increment();
        if (status != null && status.getResponseTime() != null
                && !Double.isNaN(status.getResponseTime())) {
            pollDuration.record((long) (status.getResponseTime() * 1_000_000.0),
                    java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }
}
