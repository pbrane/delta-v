/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetentionRulesValidatorTest {

    @Test
    void acceptsValidRuleSet() {
        List<Map<String, Object>> raw = List.of(
                Map.of("id", "cleared-idle-short",
                       "action", "delete",
                       "when", Map.of("severity_at_most", "NORMAL", "idle_for", "PT5M",
                                      "acked", false, "ticketed", false)));

        List<RetentionRule> rules = new RetentionRulesValidator().validate(raw);

        assertThat(rules).hasSize(1);
        RetentionRule r = rules.get(0);
        assertThat(r.id()).isEqualTo("cleared-idle-short");
        assertThat(r.action()).isEqualTo("delete");
        assertThat(r.severityAtMost()).contains(2);   // NORMAL = 2
        assertThat(r.idleFor()).contains(Duration.ofMinutes(5));
        assertThat(r.acked()).contains(false);
        assertThat(r.ticketed()).contains(false);
    }

    @Test
    void rejectsUnknownAction() {
        List<Map<String, Object>> raw = List.of(
                Map.of("id", "x", "action", "archive",
                       "when", Map.of("idle_for", "PT5M")));

        assertThatThrownBy(() -> new RetentionRulesValidator().validate(raw))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsUnknownPredicateField() {
        List<Map<String, Object>> raw = List.of(
                Map.of("id", "x", "action", "delete",
                       "when", Map.of("nonsense", "PT5M")));

        assertThatThrownBy(() -> new RetentionRulesValidator().validate(raw))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsEmptyWhenBlock() {
        List<Map<String, Object>> raw = List.of(
                Map.of("id", "x", "action", "delete",
                       "when", Map.of()));

        assertThatThrownBy(() -> new RetentionRulesValidator().validate(raw))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsMalformedIdleDuration() {
        List<Map<String, Object>> raw = List.of(
                Map.of("id", "x", "action", "delete",
                       "when", Map.of("idle_for", "5 minutes")));

        assertThatThrownBy(() -> new RetentionRulesValidator().validate(raw))
                .isInstanceOf(IllegalStateException.class);
    }
}
