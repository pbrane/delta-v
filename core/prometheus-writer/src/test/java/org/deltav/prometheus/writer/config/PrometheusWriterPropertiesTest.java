/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.config;

import org.deltav.prometheus.writer.translate.InstanceSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusWriterPropertiesTest {

    @Test
    void binds_full_yaml_shape() {
        Map<String, Object> map = Map.ofEntries(
                Map.entry("prometheus-writer.remote-write.url", "http://vm:8428/api/v1/write"),
                Map.entry("prometheus-writer.remote-write.auth.type", "bearer"),
                Map.entry("prometheus-writer.remote-write.auth.bearer-token", "abc"),
                Map.entry("prometheus-writer.remote-write.headers.X-Scope-OrgID", "tenant-1"),
                Map.entry("prometheus-writer.batch.max-samples", "1000"),
                Map.entry("prometheus-writer.batch.max-bytes", "1048576"),
                Map.entry("prometheus-writer.batch.max-interval-ms", "1000"),
                Map.entry("prometheus-writer.retry.initial-backoff-ms", "100"),
                Map.entry("prometheus-writer.retry.max-backoff-ms", "30000"),
                Map.entry("prometheus-writer.circuit-breaker.failure-rate-threshold", "50"),
                Map.entry("prometheus-writer.labels.from-metadata[0]", "requisition:region"),
                Map.entry("prometheus-writer.startup-gate.enabled", "true")
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.remoteWrite().url()).isEqualTo("http://vm:8428/api/v1/write");
        assertThat(props.remoteWrite().auth().type()).isEqualTo(PrometheusWriterProperties.AuthType.BEARER);
        assertThat(props.remoteWrite().auth().bearerToken()).isEqualTo("abc");
        assertThat(props.remoteWrite().headers()).containsEntry("X-Scope-OrgID", "tenant-1");
        assertThat(props.batch().maxSamples()).isEqualTo(1000);
        assertThat(props.batch().maxBytes()).isEqualTo(1_048_576);
        assertThat(props.labels().fromMetadata()).containsExactly("requisition:region");
        assertThat(props.startupGate().enabled()).isTrue();
    }

    @Test
    void defaults_are_sensible_when_minimal_yaml() {
        Map<String, Object> map = Map.of(
                "prometheus-writer.remote-write.url", "http://localhost/write"
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.batch().maxSamples()).isEqualTo(1000);
        assertThat(props.batch().maxBytes()).isEqualTo(1_048_576);
        assertThat(props.batch().maxIntervalMs()).isEqualTo(1000);
        assertThat(props.remoteWrite().auth().type()).isEqualTo(PrometheusWriterProperties.AuthType.NONE);
        assertThat(props.startupGate().enabled()).isTrue();
    }

    @Test
    void defaults_include_new_instance_source_and_metadata_keys() {
        Map<String, Object> map = Map.of(
                "prometheus-writer.remote-write.url", "http://localhost/write"
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.labels().instanceSource()).isEqualTo(InstanceSource.NODE_LABEL);
        assertThat(props.labels().fromMetadata())
                .containsExactly("snmp:sysContact", "snmp:sysLocation");
    }

    @Test
    void defaults_include_metrics_cardinality_tracking_enabled_with_default_cap() {
        Map<String, Object> map = Map.of(
                "prometheus-writer.remote-write.url", "http://localhost/write"
        );
        ConfigurationPropertySource src = new MapConfigurationPropertySource(map);
        PrometheusWriterProperties props = new Binder(src)
                .bind("prometheus-writer", Bindable.of(PrometheusWriterProperties.class))
                .get();

        assertThat(props.metrics().cardinalityTracking().enabled()).isTrue();
        assertThat(props.metrics().cardinalityTracking().cap()).isEqualTo(100_000);
    }
}
