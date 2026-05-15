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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

class ResponseTimePublisherTest {

    private ResponseTimeSample sample(ProducerType producer, String label) {
        return new ResponseTimeSample(42, "ICMP", "Default", 12.5, 1_700_000_000_000L,
                producer, label);
    }

    @Test
    void publishesBatchWithCorrectFieldsKeyAndProducer() throws Exception {
        StreamBridge streamBridge = mock(StreamBridge.class);
        when(streamBridge.send(any(String.class), any())).thenReturn(true);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(sample(ProducerType.PRODUCER_POLLERD, "pollerd"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<byte[]>> msg = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq("publishTimeseries-out-0"), msg.capture());

        byte[] key = (byte[]) msg.getValue().getHeaders().get(KafkaHeaders.KEY);
        assertThat(new String(key, StandardCharsets.UTF_8)).isEqualTo("Default@42");

        TimeseriesBatch batch = TimeseriesBatch.parseFrom(msg.getValue().getPayload());
        assertThat(batch.getNodeId()).isEqualTo(42);
        assertThat(batch.getLocation()).isEqualTo("Default");
        assertThat(batch.getProducer()).isEqualTo(ProducerType.PRODUCER_POLLERD);
        assertThat(batch.getResourcesCount()).isEqualTo(1);
        assertThat(batch.getResources(0).getGroups(0).getAttributes(0).getNumeric())
                .isEqualTo(12.5);

        assertThat(registry.counter("deltav_timeseries_batches_published_total",
                "location", "Default", "producer", "pollerd").count()).isEqualTo(1.0);
    }

    @Test
    void perspectiveProducerIsCarriedThrough() throws Exception {
        StreamBridge streamBridge = mock(StreamBridge.class);
        when(streamBridge.send(any(String.class), any())).thenReturn(true);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(sample(ProducerType.PRODUCER_PERSPECTIVE_POLLERD, "perspectivepollerd"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<byte[]>> msg = ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq("publishTimeseries-out-0"), msg.capture());
        TimeseriesBatch batch = TimeseriesBatch.parseFrom(msg.getValue().getPayload());
        assertThat(batch.getProducer()).isEqualTo(ProducerType.PRODUCER_PERSPECTIVE_POLLERD);
    }

    @Test
    void failedSendIncrementsFailureCounter() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        when(streamBridge.send(any(String.class), any())).thenReturn(false);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(sample(ProducerType.PRODUCER_POLLERD, "pollerd"));

        assertThat(registry.counter("deltav_timeseries_batches_failed_total",
                "location", "Default", "producer", "pollerd",
                "reason", "kafka_send_error").count()).isEqualTo(1.0);
        assertThat(registry.counter("deltav_timeseries_batches_published_total",
                "location", "Default", "producer", "pollerd").count()).isEqualTo(0.0);
    }

    @Test
    void nanResponseTimeIsSkipped() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(new ResponseTimeSample(1, "ICMP", "Default", Double.NaN,
                1_700_000_000_000L, ProducerType.PRODUCER_POLLERD, "pollerd"));

        verify(streamBridge, never()).send(any(String.class), any());
    }

    @Test
    void nullSampleIsSafelyIgnored() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ResponseTimePublisher publisher = new ResponseTimePublisher(streamBridge, registry);

        publisher.publish(null);

        verify(streamBridge, never()).send(any(String.class), any());
    }
}
