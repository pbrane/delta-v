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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.Date;

import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opennms.netmgt.collection.api.CollectionSet;
import org.opennms.netmgt.collection.api.CollectionStatus;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

class TimeseriesKafkaPublisherTest {

    private StreamBridge streamBridge;
    private CollectionSetToProtobufTranslator translator;
    private MeterRegistry meterRegistry;
    private TimeseriesKafkaPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        streamBridge = mock(StreamBridge.class);
        translator = mock(CollectionSetToProtobufTranslator.class);
        meterRegistry = new SimpleMeterRegistry();
        publisher = new TimeseriesKafkaPublisher(streamBridge, translator, meterRegistry);
        when(streamBridge.send(any(String.class), any(Message.class))).thenReturn(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void happyPathSendsOneMessageWithCorrectBindingAndKey() {
        CollectionSet set = mock(CollectionSet.class);
        when(set.getStatus()).thenReturn(CollectionStatus.SUCCEEDED);
        when(set.getCollectionTimestamp()).thenReturn(new Date(1700000000000L));
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(42).setLocation("Site-A").setCollectionPackage("default")
                .setTimestampMs(1700000000000L)
                .addResources(org.deltav.timeseries.proto.Resource.newBuilder()
                        .setResourceId("node[42]").setType("node").build())
                .build();
        when(translator.translate(set, "default", 42, "Site-A")).thenReturn(batch);

        publisher.publish(set, "default", 42, "Site-A");

        ArgumentCaptor<Message<byte[]>> captor = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq("publishTimeseries-out-0"), captor.capture());
        Message<byte[]> sent = captor.getValue();
        assertThat(sent.getPayload()).isEqualTo(batch.toByteArray());
        assertThat(sent.getHeaders().get(KafkaHeaders.KEY)).isEqualTo("Site-A@42".getBytes());

        assertThat(meterRegistry.counter("deltav_timeseries_batches_published_total",
                "location", "Site-A", "producer", "collectd").count()).isEqualTo(1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void emptyBatchSkippedWithoutSend() {
        CollectionSet set = mock(CollectionSet.class);
        when(set.getStatus()).thenReturn(CollectionStatus.SUCCEEDED);
        TimeseriesBatch empty = TimeseriesBatch.newBuilder()
                .setNodeId(42).setLocation("Site-A").build();
        when(translator.translate(set, "default", 42, "Site-A")).thenReturn(empty);

        publisher.publish(set, "default", 42, "Site-A");

        verify(streamBridge, never()).send(any(String.class), any(Message.class));
        // empty_batch is a benign skip, tracked on the skipped counter;
        // the failed counter stays at zero for empty batches so SLO alerts
        // on batches_failed_total fire only for real errors.
        assertThat(meterRegistry.counter("deltav_timeseries_batches_skipped_total",
                "location", "Site-A", "producer", "collectd", "reason", "empty_batch").count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.counter("deltav_timeseries_batches_failed_total",
                "location", "Site-A", "producer", "collectd", "reason", "empty_batch").count())
                .isEqualTo(0.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void translatorThrowingLogsAndIncrementsFailureCounter() {
        CollectionSet set = mock(CollectionSet.class);
        when(translator.translate(set, "default", 42, "Site-A"))
                .thenThrow(new RuntimeException("boom"));

        publisher.publish(set, "default", 42, "Site-A");

        verify(streamBridge, never()).send(any(String.class), any(Message.class));
        assertThat(meterRegistry.counter("deltav_timeseries_batches_failed_total",
                "location", "Site-A", "producer", "collectd", "reason", "translator_error").count())
                .isEqualTo(1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void streamBridgeReturningFalseIncrementsFailureCounter() {
        CollectionSet set = mock(CollectionSet.class);
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(1).setLocation("Default").setCollectionPackage("default")
                .addResources(org.deltav.timeseries.proto.Resource.newBuilder()
                        .setResourceId("node[1]").setType("node").build())
                .build();
        when(translator.translate(set, "default", 1, "Default")).thenReturn(batch);
        when(streamBridge.send(any(String.class), any(Message.class))).thenReturn(false);

        publisher.publish(set, "default", 1, "Default");

        assertThat(meterRegistry.counter("deltav_timeseries_batches_failed_total",
                "location", "Default", "producer", "collectd", "reason", "kafka_send_error")
                .count()).isEqualTo(1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void streamBridgeThrowingIncrementsFailureCounter() {
        CollectionSet set = mock(CollectionSet.class);
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(1).setLocation("Default").setCollectionPackage("default")
                .addResources(org.deltav.timeseries.proto.Resource.newBuilder()
                        .setResourceId("node[1]").setType("node").build())
                .build();
        when(translator.translate(set, "default", 1, "Default")).thenReturn(batch);
        when(streamBridge.send(any(String.class), any(Message.class)))
                .thenThrow(new RuntimeException("kafka unavailable"));

        publisher.publish(set, "default", 1, "Default");

        assertThat(meterRegistry.counter("deltav_timeseries_batches_failed_total",
                "location", "Default", "producer", "collectd", "reason", "kafka_send_error")
                .count()).isEqualTo(1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void oversizedBatchStillPublishedButWarningCounterIncrements() {
        CollectionSet set = mock(CollectionSet.class);
        var resourceBuilder = org.deltav.timeseries.proto.Resource.newBuilder()
                .setResourceId("node[1]").setType("node");
        var groupBuilder = org.deltav.timeseries.proto.AttributeGroup.newBuilder()
                .setName("padding");
        String bigString = "x".repeat(1000);
        for (int i = 0; i < 900; i++) {
            groupBuilder.addAttributes(org.deltav.timeseries.proto.Attribute.newBuilder()
                    .setName("attr" + i).setText(bigString)
                    .setType(org.deltav.timeseries.proto.AttributeType.ATTRIBUTE_TYPE_STRING)
                    .build());
        }
        resourceBuilder.addGroups(groupBuilder);
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(1).setLocation("Default").setCollectionPackage("default")
                .addResources(resourceBuilder).build();
        when(translator.translate(set, "default", 1, "Default")).thenReturn(batch);

        publisher.publish(set, "default", 1, "Default");

        assertThat(batch.toByteArray().length).isGreaterThan(800_000);
        verify(streamBridge).send(eq("publishTimeseries-out-0"), any(Message.class));
        assertThat(meterRegistry.counter("deltav_timeseries_batch_size_warning_total",
                "location", "Default").count()).isEqualTo(1.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void locationWithAtSignProducesValidPartitionKey() {
        CollectionSet set = mock(CollectionSet.class);
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(42).setLocation("weird@location").setCollectionPackage("default")
                .addResources(org.deltav.timeseries.proto.Resource.newBuilder()
                        .setResourceId("node[42]").setType("node").build())
                .build();
        when(translator.translate(set, "default", 42, "weird@location")).thenReturn(batch);

        publisher.publish(set, "default", 42, "weird@location");

        ArgumentCaptor<Message<byte[]>> captor = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(any(String.class), captor.capture());
        assertThat(captor.getValue().getHeaders().get(KafkaHeaders.KEY))
                .isEqualTo("weird@location@42".getBytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    void nodeIdZeroStillProducesPartitionKey() {
        CollectionSet set = mock(CollectionSet.class);
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(0).setLocation("Default").setCollectionPackage("default")
                .addResources(org.deltav.timeseries.proto.Resource.newBuilder()
                        .setResourceId("node[0]").setType("node").build())
                .build();
        when(translator.translate(set, "default", 0, "Default")).thenReturn(batch);

        publisher.publish(set, "default", 0, "Default");

        ArgumentCaptor<Message<byte[]>> captor = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(any(String.class), captor.capture());
        assertThat(captor.getValue().getHeaders().get(KafkaHeaders.KEY))
                .isEqualTo("Default@0".getBytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    void longLocationNameIsCarriedInPartitionKeyUnchanged() {
        String longLocation = "a".repeat(256);
        CollectionSet set = mock(CollectionSet.class);
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(1).setLocation(longLocation).setCollectionPackage("default")
                .addResources(org.deltav.timeseries.proto.Resource.newBuilder()
                        .setResourceId("node[1]").setType("node").build())
                .build();
        when(translator.translate(set, "default", 1, longLocation)).thenReturn(batch);

        publisher.publish(set, "default", 1, longLocation);

        ArgumentCaptor<Message<byte[]>> captor = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(any(String.class), captor.capture());
        assertThat(new String((byte[]) captor.getValue().getHeaders().get(KafkaHeaders.KEY)))
                .isEqualTo(longLocation + "@1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishDurationTimerIsRecorded() {
        CollectionSet set = mock(CollectionSet.class);
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(1).setLocation("Default").setCollectionPackage("default")
                .addResources(org.deltav.timeseries.proto.Resource.newBuilder()
                        .setResourceId("node[1]").setType("node").build())
                .build();
        when(translator.translate(set, "default", 1, "Default")).thenReturn(batch);

        publisher.publish(set, "default", 1, "Default");

        assertThat(meterRegistry.find("deltav_timeseries_publish_duration_seconds")
                .tag("location", "Default").tag("producer", "collectd").timer().count())
                .isEqualTo(1L);
    }
}
