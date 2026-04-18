/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.config;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Map;

/**
 * Strongly-typed binding for the {@code prometheus-writer.*} section of
 * {@code application.yml}. Every field in spec §8 appears here with a
 * default that matches the yaml default.
 */
@Validated
@ConfigurationProperties(prefix = "prometheus-writer")
public record PrometheusWriterProperties(
        @NotNull RemoteWrite remoteWrite,
        @NotNull Batch batch,
        @NotNull Retry retry,
        @NotNull CircuitBreaker circuitBreaker,
        @NotNull Labels labels,
        @NotNull StartupGate startupGate
) {
    public PrometheusWriterProperties {
        if (remoteWrite == null) remoteWrite = new RemoteWrite(null, new Auth(AuthType.NONE, null, null, null), Map.of());
        if (batch == null)          batch = new Batch(1000, 1_048_576, 1000);
        if (retry == null)          retry = new Retry(100, 30_000, 0.1);
        if (circuitBreaker == null) circuitBreaker = new CircuitBreaker(50, 20, 10, 30_000, 3);
        if (labels == null)         labels = new Labels(List.of());
        if (startupGate == null)    startupGate = new StartupGate(true);
    }

    public record RemoteWrite(
            @NotBlank String url,
            @NotNull Auth auth,
            Map<String, String> headers
    ) {
        public RemoteWrite {
            if (auth == null) auth = new Auth(AuthType.NONE, null, null, null);
            if (headers == null) headers = Map.of();
        }
    }

    public record Auth(
            AuthType type,
            String bearerToken,
            String basicUsername,
            String basicPassword
    ) {}

    public enum AuthType { NONE, BEARER, BASIC }

    public record Batch(
            @Positive int maxSamples,
            @Positive int maxBytes,
            @Positive long maxIntervalMs
    ) {}

    public record Retry(
            @Positive long initialBackoffMs,
            @Positive long maxBackoffMs,
            @DecimalMin("0.0") @DecimalMax("1.0") double jitterFactor
    ) {}

    public record CircuitBreaker(
            @Min(1) @Max(100) int failureRateThreshold,
            @Positive int slidingWindowSize,
            @Positive int minimumNumberOfCalls,
            @Positive long waitDurationOpenMs,
            @Positive int halfOpenPermittedCalls
    ) {}

    public record Labels(
            List<String> fromMetadata
    ) {}

    public record StartupGate(
            boolean enabled
    ) {}
}
