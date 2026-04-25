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

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.deltav.timeseries.proto.Attribute;
import org.deltav.timeseries.proto.AttributeGroup;
import org.deltav.timeseries.proto.AttributeType;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.opennms.netmgt.poller.PollStatus;
import org.opennms.netmgt.poller.pollables.PollableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Publishes per-poll response-time samples to the {@code deltav-timeseries}
 * Kafka topic as {@link TimeseriesBatch} protobuf records, keyed by
 * {@code "{location}@{nodeId}"}.
 *
 * <p>Each batch contains a single {@link Resource} whose {@code resource_id}
 * follows the horizon convention {@code node[N].monitoredService[svc]}. The
 * single attribute carries the response-time in milliseconds as a GAUGE.
 * Producer is set to {@link ProducerType#PRODUCER_POLLERD} so the consumer
 * can distinguish the origin from collectd or perspectivepollerd records.
 *
 * <p>Error-isolated: never throws. Failures bump
 * {@code deltav_timeseries_batches_failed_total} and the poll context
 * continues unaffected.
 */
public class PollResultPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(PollResultPublisher.class);
    static final String BINDING_NAME = "publishTimeseries-out-0";
    static final String GROUP_NAME = "response-time";
    static final String ATTRIBUTE_NAME = "response";
    static final String RESOURCE_TYPE = "monitoredService";

    private final StreamBridge streamBridge;
    private final MeterRegistry meterRegistry;
    private final ProducerType producerType;
    private final String producerLabel;

    public PollResultPublisher(StreamBridge streamBridge,
                               MeterRegistry meterRegistry,
                               ProducerType producerType,
                               String producerLabel) {
        this.streamBridge = streamBridge;
        this.meterRegistry = meterRegistry;
        this.producerType = producerType;
        this.producerLabel = producerLabel;
    }

    /**
     * Publishes one response-time sample. Returns silently when the poll
     * has no measurable response time (UNKNOWN polls, immediate failures
     * with no I/O).
     */
    public void publish(PollableService service, PollStatus status) {
        if (service == null || status == null) {
            return;
        }
        Double responseTime = status.getResponseTime();
        if (responseTime == null || Double.isNaN(responseTime)) {
            return;
        }
        String location = service.getNodeLocation() != null ? service.getNodeLocation() : "Default";
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            TimeseriesBatch batch = buildBatch(service, status, responseTime, location);
            byte[] payload;
            try {
                payload = batch.toByteArray();
            } catch (RuntimeException ex) {
                LOG.warn("Serialization failed for node {} svc {}; dropping",
                        service.getNodeId(), service.getSvcName(), ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producerLabel,
                        "reason", "serialization_error").increment();
                return;
            }
            byte[] key = (location + "@" + service.getNodeId()).getBytes(StandardCharsets.UTF_8);
            Message<byte[]> message = MessageBuilder.withPayload(payload)
                    .setHeader(KafkaHeaders.KEY, key)
                    .build();
            boolean sent;
            try {
                sent = streamBridge.send(BINDING_NAME, message);
            } catch (RuntimeException ex) {
                LOG.warn("streamBridge.send threw for node {} svc {}",
                        service.getNodeId(), service.getSvcName(), ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producerLabel,
                        "reason", "kafka_send_error").increment();
                return;
            }
            if (!sent) {
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producerLabel,
                        "reason", "kafka_send_error").increment();
                return;
            }
            meterRegistry.counter("deltav_timeseries_batches_published_total",
                    "location", location, "producer", producerLabel).increment();
        } finally {
            sample.stop(Timer.builder("deltav_timeseries_publish_duration_seconds")
                    .tags("location", location, "producer", producerLabel)
                    .register(meterRegistry));
        }
    }

    private TimeseriesBatch buildBatch(PollableService service, PollStatus status,
                                       double responseTimeMs, String location) {
        long timestampMs = status.getTimestamp() != null
                ? status.getTimestamp().getTime()
                : System.currentTimeMillis();
        String resourceId = "node[" + service.getNodeId() + "].monitoredService["
                + service.getSvcName() + "]";
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
                .setInstance(service.getSvcName() != null ? service.getSvcName() : "")
                .addGroups(group)
                .build();
        return TimeseriesBatch.newBuilder()
                .setTimestampMs(timestampMs)
                .setNodeId(service.getNodeId())
                .setLocation(location)
                .setCollectionPackage(service.getSvcName() != null ? service.getSvcName() : "")
                .setProducer(producerType)
                .addResources(resource)
                .build();
    }
}
