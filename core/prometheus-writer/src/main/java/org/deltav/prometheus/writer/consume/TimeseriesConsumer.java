/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.consume;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.deltav.prometheus.writer.metrics.PrometheusWriterMetrics;
import org.deltav.prometheus.writer.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.rw.BatchingRwWriter;
import org.deltav.prometheus.writer.translate.PromSample;
import org.deltav.prometheus.writer.translate.TimeseriesToPromTranslator;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Configuration
public class TimeseriesConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesConsumer.class);

    @Bean
    public Consumer<Message<byte[]>> timeseriesConsumer(
            NodeContextCache cache,
            TimeseriesToPromTranslator translator,
            BatchingRwWriter writer,
            MeterRegistry metrics) {
        Counter consumed = metrics.counter(PrometheusWriterMetrics.RECORDS_CONSUMED);
        Counter samplesIn = metrics.counter(PrometheusWriterMetrics.SAMPLES_IN);
        Counter parseErrors = metrics.counter(PrometheusWriterMetrics.RECORDS_PARSE_ERRORS);
        return message -> {
            try {
                TimeseriesBatch batch = TimeseriesBatch.parseFrom(message.getPayload());
                String key = batch.getLocation() + "@" + batch.getNodeId();
                Optional<NodeContext> nc = cache.get(key);
                List<PromSample> samples = translator.translate(batch, nc);
                consumed.increment();
                samplesIn.increment(samples.size());
                samples.forEach(writer::add);
            } catch (Exception e) {
                LOG.warn("Failed to process TimeseriesBatch — dropping", e);
                parseErrors.increment();
            }
        };
    }
}
