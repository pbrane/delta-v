/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.alarms.materializer.config.MaterializerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class RetentionEngineTest {

    @Test
    void producesTombstonesForMatchingRowsAndDoesNotDelete() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class), any(), any()))
                .thenReturn(List.of("rk-1", "rk-2"));

        MockProducer<String, byte[]> producer = new MockProducer<>(
                true, null, new StringSerializer(), new ByteArraySerializer());
        MaterializerProperties props = new MaterializerProperties();

        RetentionRule rule = RetentionRule.fromMap(java.util.Map.of(
                "id", "cleared-idle", "action", "delete",
                "when", java.util.Map.of("severity_at_most", "NORMAL", "idle_for", "PT5M")));

        Clock fixed = Clock.fixed(Instant.parse("2026-05-22T12:00:00Z"), ZoneOffset.UTC);
        PredicateEvaluator evaluator = new PredicateEvaluator(fixed);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        RetentionEngine engine = new RetentionEngine(props, jdbc, producer, evaluator, List.of(rule), registry);
        engine.runOnce();

        // Two tombstones produced.
        assertThat(producer.history()).hasSize(2);
        assertThat(producer.history()).allMatch(r -> r.value() == null);
        assertThat(producer.history().stream().map(ProducerRecord::key))
                .containsExactly("rk-1", "rk-2");

        // PG was queried but never written.
        verify(jdbc, atLeastOnce()).queryForList(anyString(), eq(String.class), any(), any());
        verify(jdbc, never()).update(anyString(), any(Object[].class));
        verify(jdbc, never()).update(anyString());
    }

    @Test
    void emptyMatchSetIsBenign() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class), any(), any())).thenReturn(List.of());

        MockProducer<String, byte[]> producer = new MockProducer<>(
                true, null, new StringSerializer(), new ByteArraySerializer());

        RetentionRule rule = RetentionRule.fromMap(java.util.Map.of(
                "id", "x", "action", "delete",
                "when", java.util.Map.of("idle_for", "P1D")));

        new RetentionEngine(new MaterializerProperties(), jdbc, producer,
                new PredicateEvaluator(Clock.systemUTC()),
                List.of(rule), new SimpleMeterRegistry())
                .runOnce();

        assertThat(producer.history()).isEmpty();
    }
}
