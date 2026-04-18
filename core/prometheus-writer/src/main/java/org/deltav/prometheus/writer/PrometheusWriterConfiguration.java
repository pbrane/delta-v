/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/**
 * Top-level Spring configuration for the prometheus-writer module.
 *
 * <p>Provides the {@link RestClient.Builder} used by
 * {@link org.deltav.prometheus.writer.rw.RemoteWriteHttpClient} with the
 * spec §5 timeouts (5s connect, 30s read), and registers
 * {@link PrometheusWriterProperties} as a Boot {@code @ConfigurationProperties}
 * binding target.</p>
 */
@Configuration
@EnableConfigurationProperties(PrometheusWriterProperties.class)
public class PrometheusWriterConfiguration {

    /** Spec §5: connect timeout 5s, read timeout 30s. */
    @Bean
    public RestClient.Builder restClientBuilder() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        factory.setReadTimeout((int) Duration.ofSeconds(30).toMillis());
        return RestClient.builder().requestFactory(factory);
    }
}
