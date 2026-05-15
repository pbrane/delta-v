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
package org.deltav.poller.timeseries;

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.deltav.timeseries.proto.Attribute;
import org.deltav.timeseries.proto.AttributeGroup;
import org.deltav.timeseries.proto.AttributeType;
import org.deltav.timeseries.proto.Resource;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Shared publisher of per-poll response-time samples to the
 * {@code deltav-timeseries} Kafka topic as {@link TimeseriesBatch} protobuf
 * records, keyed {@code "{location}@{nodeId}"}.
 *
 * <p>Used by both Pollerd and PerspectivePollerd. Each daemon adapts its
 * horizon-specific poll object into a {@link ResponseTimeSample}; this class
 * holds the entire publish mechanism, identical for both. Producer identity
 * (protobuf {@code ProducerType} + Micrometer {@code producer} label) is
 * carried on the sample, supplied by the daemon-side adapter.
 *
 * <p>Each batch carries a single {@link Resource}
 * ({@code resource_id = node[N].monitoredService[svc]}) with a single
 * GAUGE {@link Attribute} holding the response time in milliseconds.
 *
 * <p>Error-isolated: never throws. Failures bump
 * {@code deltav_timeseries_batches_failed_total} and the caller continues.
 */
public class ResponseTimePublisher {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseTimePublisher.class);
    static final String BINDING_NAME = "publishTimeseries-out-0";
    static final String GROUP_NAME = "response-time";
    static final String ATTRIBUTE_NAME = "response";
    static final String RESOURCE_TYPE = "monitoredService";

    private final StreamBridge streamBridge;
    private final MeterRegistry meterRegistry;

    public ResponseTimePublisher(StreamBridge streamBridge, MeterRegistry meterRegistry) {
        this.streamBridge = streamBridge;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Publishes one response-time sample. Returns silently for a null sample
     * or a NaN response time (defensive — daemon adapters already guard).
     */
    public void publish(ResponseTimeSample sample) {
        if (sample == null || Double.isNaN(sample.responseTimeMs())) {
            return;
        }
        String location = sample.location() != null ? sample.location() : "Default";
        String producer = sample.producerLabel();
        Timer.Sample timer = Timer.start(meterRegistry);
        try {
            TimeseriesBatch batch = buildBatch(sample, location);
            byte[] payload;
            try {
                payload = batch.toByteArray();
            } catch (RuntimeException ex) {
                LOG.warn("Serialization failed for node {} svc {}; dropping",
                        sample.nodeId(), sample.serviceName(), ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producer,
                        "reason", "serialization_error").increment();
                return;
            }
            byte[] key = (location + "@" + sample.nodeId()).getBytes(StandardCharsets.UTF_8);
            Message<byte[]> message = MessageBuilder.withPayload(payload)
                    .setHeader(KafkaHeaders.KEY, key)
                    .build();
            boolean sent;
            try {
                sent = streamBridge.send(BINDING_NAME, message);
            } catch (RuntimeException ex) {
                LOG.warn("streamBridge.send threw for node {} svc {}",
                        sample.nodeId(), sample.serviceName(), ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producer,
                        "reason", "kafka_send_error").increment();
                return;
            }
            if (!sent) {
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", location, "producer", producer,
                        "reason", "kafka_send_error").increment();
                return;
            }
            meterRegistry.counter("deltav_timeseries_batches_published_total",
                    "location", location, "producer", producer).increment();
        } finally {
            timer.stop(Timer.builder("deltav_timeseries_publish_duration_seconds")
                    .tags("location", location, "producer", producer)
                    .register(meterRegistry));
        }
    }

    private TimeseriesBatch buildBatch(ResponseTimeSample sample, String location) {
        String svc = sample.serviceName() != null ? sample.serviceName() : "";
        String resourceId = "node[" + sample.nodeId() + "].monitoredService[" + svc + "]";
        Attribute responseAttr = Attribute.newBuilder()
                .setName(ATTRIBUTE_NAME)
                .setNumeric(sample.responseTimeMs())
                .setType(AttributeType.ATTRIBUTE_TYPE_GAUGE)
                .build();
        AttributeGroup group = AttributeGroup.newBuilder()
                .setName(GROUP_NAME)
                .addAttributes(responseAttr)
                .build();
        Resource resource = Resource.newBuilder()
                .setResourceId(resourceId)
                .setType(RESOURCE_TYPE)
                .setInstance(svc)
                .addGroups(group)
                .build();
        return TimeseriesBatch.newBuilder()
                .setTimestampMs(sample.timestampMs())
                .setNodeId(sample.nodeId())
                .setLocation(location)
                .setCollectionPackage(svc)
                .setProducer(sample.producerType())
                .addResources(resource)
                .build();
    }
}
