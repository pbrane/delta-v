/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.translate.PromSample;
import org.junit.jupiter.api.Test;
import prometheus.prompb.WriteRequest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BatchingRwWriterTest {

    private PrometheusWriterProperties props(int maxSamples, int maxBytes, long maxIntervalMs) {
        return new PrometheusWriterProperties(
                new PrometheusWriterProperties.RemoteWrite("http://test/write",
                    new PrometheusWriterProperties.Auth(PrometheusWriterProperties.AuthType.NONE, null, null, null), Map.of()),
                new PrometheusWriterProperties.Batch(maxSamples, maxBytes, maxIntervalMs),
                null, null, null, null);
    }

    private PromSample sample(String name) {
        return new PromSample(name, Map.of("k", "v"), 1.0, 1_700_000_000_000L);
    }

    @Test
    void flushes_on_max_samples_threshold() throws Exception {
        WriteRequestBuilder builder = mock(WriteRequestBuilder.class);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(builder.build(any())).thenReturn(WriteRequest.newBuilder().build());
        when(http.post(any())).thenReturn(200);
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        BatchingRwWriter w = new BatchingRwWriter(props(3, 100_000_000, 60_000), builder, http, reg);

        w.add(sample("a"));
        w.add(sample("b"));
        verifyNoInteractions(http);  // not flushed yet
        w.add(sample("c"));  // hits maxSamples=3
        verify(http, times(1)).post(any());
        assertThat(reg.find("deltav.prometheus.writer.batches.sent").counter().count()).isEqualTo(1.0);
        assertThat(reg.find("deltav.prometheus.writer.samples.sent").counter().count()).isEqualTo(3.0);
    }

    @Test
    void flushes_on_max_bytes_threshold() throws Exception {
        WriteRequestBuilder builder = mock(WriteRequestBuilder.class);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(builder.build(any())).thenReturn(WriteRequest.newBuilder().build());
        when(http.post(any())).thenReturn(200);
        // Tiny maxBytes so a single sample will trigger
        BatchingRwWriter w = new BatchingRwWriter(props(10_000, 5, 60_000), builder, http, new SimpleMeterRegistry());
        w.add(sample("metric_name_long_enough_to_exceed_5_bytes"));
        verify(http, times(1)).post(any());
    }

    @Test
    void flush_calls_http_with_grouped_WriteRequest() throws Exception {
        WriteRequestBuilder builder = mock(WriteRequestBuilder.class);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        WriteRequest stub = WriteRequest.newBuilder().build();
        when(builder.build(any())).thenReturn(stub);
        when(http.post(any())).thenReturn(200);
        BatchingRwWriter w = new BatchingRwWriter(props(2, 100_000_000, 60_000), builder, http, new SimpleMeterRegistry());
        w.add(sample("a"));
        w.add(sample("b"));
        verify(builder, times(1)).build(argThat(list -> list.size() == 2));
        verify(http, times(1)).post(stub);
    }

    @Test
    void flushes_on_time_interval_via_scheduled_check() throws Exception {
        WriteRequestBuilder builder = mock(WriteRequestBuilder.class);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        when(builder.build(any())).thenReturn(WriteRequest.newBuilder().build());
        when(http.post(any())).thenReturn(200);
        // Very short maxIntervalMs so the scheduled-check fires quickly
        BatchingRwWriter w = new BatchingRwWriter(props(10_000, 100_000_000, 50), builder, http, new SimpleMeterRegistry());
        w.add(sample("a"));
        Thread.sleep(120);
        w.flushIfStale();  // simulate the @Scheduled tick
        verify(http, times(1)).post(any());
    }

    @Test
    void flush_empty_buffer_is_noop() {
        WriteRequestBuilder builder = mock(WriteRequestBuilder.class);
        RemoteWriteHttpClient http = mock(RemoteWriteHttpClient.class);
        BatchingRwWriter w = new BatchingRwWriter(props(10, 100_000, 60_000), builder, http, new SimpleMeterRegistry());
        w.flushIfStale();   // empty
        w.flushNow();       // empty
        verifyNoInteractions(http);
        verifyNoInteractions(builder);
    }
}
