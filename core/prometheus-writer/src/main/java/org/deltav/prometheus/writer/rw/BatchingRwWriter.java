/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.prometheus.writer.translate.PromSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import prometheus.prompb.WriteRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class BatchingRwWriter {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingRwWriter.class);

    private final PrometheusWriterProperties props;
    private final WriteRequestBuilder builder;
    private final RemoteWriteHttpClient http;
    private final Counter batchesSent;
    private final Counter samplesSent;
    private final DistributionSummary batchBytes;
    private final DistributionSummary batchSampleCount;
    private final Timer flushDuration;

    private final ReentrantLock lock = new ReentrantLock();
    private final List<PromSample> buffer = new ArrayList<>();
    private final AtomicLong approxBytes = new AtomicLong(0);
    private volatile long firstSampleAtMs = 0L;

    public BatchingRwWriter(PrometheusWriterProperties props, WriteRequestBuilder builder,
                            RemoteWriteHttpClient http, MeterRegistry reg) {
        this.props = props; this.builder = builder; this.http = http;
        String endpoint = props.remoteWrite().url();
        this.batchesSent = Counter.builder("deltav.prometheus.writer.batches.sent")
                .tag("endpoint", endpoint).register(reg);
        this.samplesSent = Counter.builder("deltav.prometheus.writer.samples.sent")
                .tag("endpoint", endpoint).register(reg);
        this.batchBytes = DistributionSummary.builder("deltav.prometheus.writer.batch.size.bytes")
                .tag("endpoint", endpoint).register(reg);
        this.batchSampleCount = DistributionSummary.builder("deltav.prometheus.writer.batch.sample.count")
                .tag("endpoint", endpoint).register(reg);
        this.flushDuration = Timer.builder("deltav.prometheus.writer.flush.duration")
                .tag("endpoint", endpoint).register(reg);
    }

    public void add(PromSample sample) {
        lock.lock();
        try {
            if (buffer.isEmpty()) firstSampleAtMs = System.currentTimeMillis();
            buffer.add(sample);
            approxBytes.addAndGet(estimateSize(sample));
            if (shouldFlushNow()) flushNow();
        } finally { lock.unlock(); }
    }

    @Scheduled(fixedDelay = 100)
    public void flushIfStale() {
        lock.lock();
        try {
            if (!buffer.isEmpty()
                    && System.currentTimeMillis() - firstSampleAtMs >= props.batch().maxIntervalMs()) {
                flushNow();
            }
        } finally { lock.unlock(); }
    }

    public void flushNow() {
        if (buffer.isEmpty()) return;
        List<PromSample> toSend = new ArrayList<>(buffer);
        buffer.clear();
        approxBytes.set(0);
        Timer.Sample sample = Timer.start();
        try {
            WriteRequest req = builder.build(toSend);
            int bytesEstimate = req.getSerializedSize();
            http.post(req);
            batchesSent.increment();
            samplesSent.increment(toSend.size());
            batchBytes.record(bytesEstimate);
            batchSampleCount.record(toSend.size());
        } catch (Exception e) {
            // Flush failures propagate to caller's retry/DLQ/circuit logic (Task 14/16).
            throw new RuntimeException(e);
        } finally {
            sample.stop(flushDuration);
        }
    }

    private boolean shouldFlushNow() {
        return buffer.size() >= props.batch().maxSamples()
                || approxBytes.get() >= props.batch().maxBytes();
    }

    private long estimateSize(PromSample s) {
        long total = 16;
        total += s.name().length();
        for (var e : s.labels().entrySet()) total += e.getKey().length() + e.getValue().length() + 2;
        return total;
    }
}
