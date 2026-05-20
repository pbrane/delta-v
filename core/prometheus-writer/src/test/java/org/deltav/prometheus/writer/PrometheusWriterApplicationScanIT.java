/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.apache.kafka.clients.admin.NewTopic;
import org.deltav.prometheus.writer.consume.TimeseriesConsumerConfiguration;
import org.deltav.prometheus.writer.dlq.DlqPublisher;
import org.deltav.prometheus.writer.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.nodecontext.NodeContextCacheHealthIndicator;
import org.deltav.prometheus.writer.nodecontext.NodeContextKafkaBootstrap;
import org.deltav.prometheus.writer.nodeinfo.NodeInfoMetricEmitter;
import org.deltav.prometheus.writer.rw.BatchingRwWriter;
import org.deltav.prometheus.writer.rw.ConsumerPauseListener;
import org.deltav.prometheus.writer.rw.RemoteWriteHttpClient;
import org.deltav.prometheus.writer.rw.RemoteWriteRetryPolicy;
import org.deltav.prometheus.writer.rw.WriteRequestBuilder;
import org.deltav.prometheus.writer.startup.TimeseriesBindingResumer;
import org.deltav.prometheus.writer.startup.TimeseriesBindingStartupGate;
import org.deltav.prometheus.writer.metrics.LabelCardinalityTracker;
import org.deltav.prometheus.writer.translate.InstanceLabelResolver;
import org.deltav.prometheus.writer.translate.LabelBuilder;
import org.deltav.prometheus.writer.translate.NameSanitizer;
import org.deltav.prometheus.writer.translate.TimeseriesToPromTranslator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-main-class integration test. Boots {@link PrometheusWriterApplication}
 * via {@code SpringApplication.run()} against a Testcontainers Kafka broker
 * and asserts every key bean from the Phase 2 spec is resolvable.
 *
 * <p><strong>Scar prophylactic for the Phase 0 #170 bug:</strong> unit tests
 * and {@code @Import}-based ITs bypass the component scan. A mistyped
 * {@code scanBasePackages} silently hides an entire {@code @Configuration}
 * class and the problem only surfaces when Docker boots the real jar. This
 * IT would have caught it in one CI run.
 */
@Testcontainers
@SpringBootTest(
        classes = PrometheusWriterApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PrometheusWriterApplicationScanIT {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"))
                    .withStartupTimeout(Duration.ofSeconds(120));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry reg) {
        reg.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        reg.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
        reg.add("prometheus-writer.remote-write.url", () -> "http://localhost:1/unused");
    }

    @Autowired ApplicationContext ctx;

    @Test
    void all_key_beans_resolve() {
        // Node-context subsystem
        assertThat(ctx.getBean(NodeContextCache.class)).isNotNull();
        assertThat(ctx.getBean(NodeContextKafkaBootstrap.class)).isNotNull();
        assertThat(ctx.getBean(NodeContextCacheHealthIndicator.class)).isNotNull();
        assertThat(ctx.getBean(NodeInfoMetricEmitter.class)).isNotNull();
        // Translation
        assertThat(ctx.getBean(NameSanitizer.class)).isNotNull();
        assertThat(ctx.getBean(LabelBuilder.class)).isNotNull();
        assertThat(ctx.getBean(InstanceLabelResolver.class)).isNotNull();
        assertThat(ctx.getBean(LabelCardinalityTracker.class)).isNotNull();
        assertThat(ctx.getBean(TimeseriesToPromTranslator.class)).isNotNull();
        // RW output
        assertThat(ctx.getBean(WriteRequestBuilder.class)).isNotNull();
        assertThat(ctx.getBean(RemoteWriteHttpClient.class)).isNotNull();
        assertThat(ctx.getBean(RemoteWriteRetryPolicy.class)).isNotNull();
        assertThat(ctx.getBean(BatchingRwWriter.class)).isNotNull();
        assertThat(ctx.getBean(CircuitBreaker.class)).isNotNull();
        assertThat(ctx.getBean(ConsumerPauseListener.class)).isNotNull();
        // Consumer + startup gate
        assertThat(ctx.getBean(TimeseriesConsumerConfiguration.class)).isNotNull();
        assertThat(ctx.getBean(TimeseriesBindingStartupGate.class)).isNotNull();
        assertThat(ctx.getBean(TimeseriesBindingResumer.class)).isNotNull();
        // DLQ
        assertThat(ctx.getBean(DlqPublisher.class)).isNotNull();
        assertThat(ctx.getBean("deltavPrometheusWriterDlqTopic", NewTopic.class)).isNotNull();
    }
}
