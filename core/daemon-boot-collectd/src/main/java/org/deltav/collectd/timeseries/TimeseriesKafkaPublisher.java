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
package org.deltav.collectd.timeseries;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.nio.charset.StandardCharsets;

import org.deltav.timeseries.proto.TimeseriesBatch;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Orchestration layer: translate a CollectionSet to protobuf, serialize, build
 * the partition key as "{location}@{nodeId}", and invoke
 * {@link StreamBridge#send(String, Message)} on the "publishTimeseries-out-0"
 * binding. Error-isolated: never throws to the caller so the persister chain
 * is unaffected by Kafka hiccups.
 *
 * <p>The caller (typically {@link TimeseriesKafkaPersister}) supplies
 * {@code nodeId} and {@code location} from its own agent context —
 * horizon's CollectionSet interface does not expose a public
 * CollectionAgent accessor, so identity must come from the caller.</p>
 */
public class TimeseriesKafkaPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesKafkaPublisher.class);
    private static final String BINDING_NAME = "publishTimeseries-out-0";
    static final int SIZE_WARNING_THRESHOLD_BYTES = 800_000;

    private final StreamBridge streamBridge;
    private final CollectionSetToProtobufTranslator translator;
    private final MeterRegistry meterRegistry;

    public TimeseriesKafkaPublisher(StreamBridge streamBridge,
                                     CollectionSetToProtobufTranslator translator,
                                     MeterRegistry meterRegistry) {
        this.streamBridge = streamBridge;
        this.translator = translator;
        this.meterRegistry = meterRegistry;
    }

    public void publish(CollectionSet set, String collectionPackage, int nodeId, String location) {
        Timer.Sample sample = Timer.start(meterRegistry);
        String loc = location != null ? location : "";
        try {
            TimeseriesBatch batch;
            try {
                batch = translator.translate(set, collectionPackage, nodeId, loc);
            } catch (RuntimeException ex) {
                LOG.warn("Translator failed for node {} / package {}; dropping CollectionSet",
                        nodeId, collectionPackage, ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", loc, "producer", "collectd",
                        "reason", "translator_error").increment();
                return;
            }

            if (batch.getResourcesCount() == 0) {
                // An empty batch is an expected outcome, not a failure: it
                // means the translator found no numeric resources in the
                // CollectionSet (e.g. a collection cycle that produced only
                // string attributes, or a node whose SNMP response was all
                // filtered out). Publishing an empty batch wastes a Kafka
                // record, so we skip. Track on the skipped counter so SLO
                // alerts on batches_failed_total stay clean for real errors
                // (translator_error, serialization_error, send_error) while
                // ops still see the skip rate here.
                meterRegistry.counter("deltav_timeseries_batches_skipped_total",
                        "location", loc, "producer", "collectd",
                        "reason", "empty_batch").increment();
                return;
            }

            byte[] payload;
            try {
                payload = batch.toByteArray();
            } catch (RuntimeException ex) {
                LOG.warn("Serialization failed for node {} / package {}",
                        batch.getNodeId(), collectionPackage, ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", loc, "producer", "collectd",
                        "reason", "serialization_error").increment();
                return;
            }

            if (payload.length > SIZE_WARNING_THRESHOLD_BYTES) {
                LOG.warn("Oversized TimeseriesBatch: {} bytes for node {} in package {}. "
                        + "Kafka max.request.size default is 1 MB. Consider reducing poll "
                        + "scope in collectd-configuration.xml.",
                        payload.length, batch.getNodeId(), batch.getCollectionPackage());
                meterRegistry.counter("deltav_timeseries_batch_size_warning_total",
                        "location", loc).increment();
            }

            DistributionSummary.builder("deltav_timeseries_batch_size_bytes")
                    .tags("location", loc, "producer", "collectd")
                    .register(meterRegistry).record(payload.length);
            DistributionSummary.builder("deltav_timeseries_resources_per_batch")
                    .tags("location", loc, "producer", "collectd")
                    .register(meterRegistry).record(batch.getResourcesCount());

            byte[] key = (batch.getLocation() + "@" + batch.getNodeId()).getBytes(StandardCharsets.UTF_8);
            Message<byte[]> message = MessageBuilder.withPayload(payload)
                    .setHeader(KafkaHeaders.KEY, key)
                    .build();

            boolean sent;
            try {
                sent = streamBridge.send(BINDING_NAME, message);
            } catch (RuntimeException ex) {
                LOG.warn("streamBridge.send threw for node {} / package {}",
                        batch.getNodeId(), collectionPackage, ex);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", loc, "producer", "collectd",
                        "reason", "kafka_send_error").increment();
                return;
            }
            if (!sent) {
                LOG.warn("streamBridge.send returned false for node {} / package {}",
                        batch.getNodeId(), collectionPackage);
                meterRegistry.counter("deltav_timeseries_batches_failed_total",
                        "location", loc, "producer", "collectd",
                        "reason", "kafka_send_error").increment();
                return;
            }

            meterRegistry.counter("deltav_timeseries_batches_published_total",
                    "location", loc, "producer", "collectd").increment();
        } finally {
            sample.stop(Timer.builder("deltav_timeseries_publish_duration_seconds")
                    .tags("location", loc, "producer", "collectd")
                    .register(meterRegistry));
        }
    }
}
