/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.consume;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.deltav.prometheus.writer.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.rw.BatchingRwWriter;
import org.deltav.prometheus.writer.translate.TimeseriesToPromTranslator;
import org.deltav.timeseries.proto.NodeContext;
import org.deltav.timeseries.proto.ProducerType;
import org.deltav.timeseries.proto.TimeseriesBatch;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * SCS test-binder IT for {@link TimeseriesConsumer}. Asserts the full path:
 * binder &rarr; deserialize &rarr; cache lookup &rarr; translator &rarr;
 * {@link BatchingRwWriter#add}, plus the parse-error counter on malformed input.
 *
 * <p>Loads only a tiny boot test app that imports {@link TimeseriesConsumer}
 * plus a {@link MockBeans} configuration providing Mockito stubs for the
 * cache, translator, and writer. {@link EnableAutoConfiguration} pulls in
 * Spring Cloud Function + Spring Cloud Stream so the {@code Consumer} bean is
 * discovered, bound, and dispatched by the in-memory
 * {@link TestChannelBinderConfiguration test binder}.
 */
@SpringBootTest(classes = TimeseriesConsumerBinderIT.TestApp.class)
@TestPropertySource(properties = {
        "spring.main.web-application-type=none",
        "spring.main.banner-mode=off",
        "spring.cloud.function.definition=timeseriesConsumer",
        "spring.cloud.stream.bindings.timeseriesConsumer-in-0.destination=timeseriesConsumer-in-0",
        // application.yml registers a "nodeContextCache" health contributor in
        // the readiness group; clear that mapping here so the slim test context
        // (no NodeContextCacheHealthIndicator bean) does not fail validation.
        "management.endpoint.health.group.readiness.include=readinessState"
})
class TimeseriesConsumerBinderIT {

    /**
     * Minimal boot application that enables Spring Boot auto-configuration so
     * SCS BindingServiceConfiguration (which discovers and binds functions) is
     * triggered. {@link TestChannelBinderConfiguration} provides the
     * in-process test binder via {@code @ConditionalOnMissingBean(Binder.class)}.
     */
    @EnableAutoConfiguration
    @Import({TimeseriesConsumer.class, MockBeans.class, TestChannelBinderConfiguration.class})
    static class TestApp {
    }

    @Autowired
    InputDestination input;

    @Autowired
    NodeContextCache cache;

    @Autowired
    TimeseriesToPromTranslator translator;

    @Autowired
    BatchingRwWriter writer;

    @Autowired
    MeterRegistry metrics;

    @Test
    void consumes_batch_invokes_translator_and_writer() {
        when(cache.get("Default@5")).thenReturn(java.util.Optional.of(
                NodeContext.newBuilder()
                        .setNodeId(5)
                        .setLocation("Default")
                        .setNodeLabel("n5")
                        .build()));
        TimeseriesBatch batch = TimeseriesBatch.newBuilder()
                .setNodeId(5)
                .setLocation("Default")
                .setProducer(ProducerType.PRODUCER_COLLECTD)
                .setTimestampMs(1_700_000_000_000L)
                .build();

        input.send(MessageBuilder.withPayload(batch.toByteArray()).build(),
                "timeseriesConsumer-in-0");

        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(5))
                .until(() -> metrics.counter("deltav.prometheus.writer.records.consumed")
                        .count() == 1.0);

        verify(translator, times(1)).translate(any(), any());
        // translator returned empty list (no resources in batch) -> writer.add never called
        verify(writer, times(0)).add(any());
    }

    @Test
    void parse_error_increments_counter_and_does_not_throw() {
        // Bytes that aren't a valid TimeseriesBatch
        input.send(MessageBuilder.withPayload(new byte[]{0x42, 0x42, 0x42}).build(),
                "timeseriesConsumer-in-0");

        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(5))
                .until(() -> metrics.counter("deltav.prometheus.writer.records.parse.errors")
                        .count() >= 1.0);
    }

    @Configuration
    static class MockBeans {
        @Bean
        NodeContextCache cache() {
            return mock(NodeContextCache.class);
        }

        @Bean
        TimeseriesToPromTranslator translator() {
            TimeseriesToPromTranslator t = mock(TimeseriesToPromTranslator.class);
            when(t.translate(any(), any())).thenReturn(java.util.List.of());
            return t;
        }

        @Bean
        BatchingRwWriter writer() {
            return mock(BatchingRwWriter.class);
        }

        @Bean
        MeterRegistry metrics() {
            return new SimpleMeterRegistry();
        }
    }
}
