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

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.deltav.timeseries.proto.Attribute;
import org.deltav.timeseries.proto.AttributeGroup;
import org.deltav.timeseries.proto.AttributeType;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.opennms.netmgt.perspectivepoller.PerspectivePolledService;
import org.opennms.netmgt.poller.PollStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Publishes per-poll response-time samples for PerspectivePollerd to the
 * {@code deltav-timeseries} Kafka topic as {@link TimeseriesBatch} protobuf
 * records, keyed by {@code "{perspective}@{nodeId}"}.
 *
 * <p>Mirrors the wire shape of {@code PollResultPublisher} (the Pollerd
 * counterpart), with two distinguishing fields:
 * <ul>
 *   <li>{@link ProducerType#PRODUCER_PERSPECTIVE_POLLERD} so the consumer
 *       can disambiguate from direct Pollerd records when the same node /
 *       service is polled by both daemons.</li>
 *   <li>{@code location} is set to {@link PerspectivePolledService#getPerspectiveLocation()}
 *       — the Minion vantage point that ran the poll. Perspective polling's
 *       whole point is the same service measured from multiple locations;
 *       encoding the perspective in {@code location} produces a clean
 *       per-vantage-point series in VictoriaMetrics.</li>
 * </ul>
 *
 * <p>Error-isolated: never throws. Failures bump
 * {@code deltav_timeseries_batches_failed_total} and the poll callback
 * continues unaffected.
 */
public class PerspectiveResponseTimePublisher {

    private static final Logger LOG = LoggerFactory.getLogger(PerspectiveResponseTimePublisher.class);
    static final String BINDING_NAME = "publishTimeseries-out-0";
    static final String GROUP_NAME = "response-time";
    static final String ATTRIBUTE_NAME = "response";
    static final String RESOURCE_TYPE = "monitoredService";
    static final String PRODUCER_LABEL = "perspectivepollerd";

    private final StreamBridge streamBridge;
    private final MeterRegistry meterRegistry;

    public PerspectiveResponseTimePublisher(StreamBridge streamBridge, MeterRegistry meterRegistry) {
        this.streamBridge = streamBridge;
        this.meterRegistry = meterRegistry;
    }

    public void publish(PerspectivePolledService polledService, PollStatus status) {
        if (polledService == null || status == null) {
            return;
        }
        Double responseTime = status.getResponseTime();
        if (responseTime == null || Double.isNaN(responseTime)) {
            return;
        }
        String perspective = polledService.getPerspectiveLocation() != null
                ? polledService.getPerspectiveLocation() : "Default";
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            TimeseriesBatch batch = buildBatch(polledService, status, responseTime, perspective);
            byte[] payload;
            try {
                payload = batch.toByteArray();
            } catch (RuntimeException ex) {
                LOG.warn("Serialization failed for node {} svc {} perspective {}; dropping",
                        polledService.getNodeId(), polledService.getServiceName(), perspective, ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", perspective, "producer", PRODUCER_LABEL,
                        "reason", "serialization_error").increment();
                return;
            }
            byte[] key = (perspective + "@" + polledService.getNodeId()).getBytes(StandardCharsets.UTF_8);
            Message<byte[]> message = MessageBuilder.withPayload(payload)
                    .setHeader(KafkaHeaders.KEY, key)
                    .build();
            boolean sent;
            try {
                sent = streamBridge.send(BINDING_NAME, message);
            } catch (RuntimeException ex) {
                LOG.warn("streamBridge.send threw for node {} svc {} perspective {}",
                        polledService.getNodeId(), polledService.getServiceName(), perspective, ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", perspective, "producer", PRODUCER_LABEL,
                        "reason", "kafka_send_error").increment();
                return;
            }
            if (!sent) {
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", perspective, "producer", PRODUCER_LABEL,
                        "reason", "kafka_send_error").increment();
                return;
            }
            meterRegistry.counter("deltav_timeseries_batches_published_total",
                    "location", perspective, "producer", PRODUCER_LABEL).increment();
        } finally {
            sample.stop(Timer.builder("deltav_timeseries_publish_duration_seconds")
                    .tags("location", perspective, "producer", PRODUCER_LABEL)
                    .register(meterRegistry));
        }
    }

    private TimeseriesBatch buildBatch(PerspectivePolledService svc, PollStatus status,
                                       double responseTimeMs, String perspective) {
        long timestampMs = status.getTimestamp() != null
                ? status.getTimestamp().getTime()
                : System.currentTimeMillis();
        String resourceId = "node[" + svc.getNodeId() + "].monitoredService["
                + svc.getServiceName() + "]";
        Attribute responseAttr = Attribute.newBuilder()
                .setName(ATTRIBUTE_NAME)
                .setNumeric(responseTimeMs)
                .setType(AttributeType.ATTRIBUTE_TYPE_GAUGE)
                .build();
        AttributeGroup group = AttributeGroup.newBuilder()
                .setName(GROUP_NAME)
                .addAttributes(responseAttr)
                .build();
        Resource resource = Resource.newBuilder()
                .setResourceId(resourceId)
                .setType(RESOURCE_TYPE)
                .setInstance(svc.getServiceName() != null ? svc.getServiceName() : "")
                .addGroups(group)
                .build();
        return TimeseriesBatch.newBuilder()
                .setTimestampMs(timestampMs)
                .setNodeId(svc.getNodeId())
                .setLocation(perspective)
                .setCollectionPackage(svc.getServiceName() != null ? svc.getServiceName() : "")
                .setProducer(ProducerType.PRODUCER_PERSPECTIVE_POLLERD)
                .addResources(resource)
                .build();
    }
}
