/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PredicateEvaluatorTest {

    private static final Clock FIXED = Clock.fixed(Instant.parse("2026-05-22T12:00:00Z"), ZoneOffset.UTC);

    private RetentionRule rule(Map<String, Object> when) {
        return RetentionRule.fromMap(Map.of(
                "id", "test", "action", "delete", "when", when));
    }

    @Test
    void severityAtMostAndIdleFor() {
        PredicateEvaluator.Compiled compiled = new PredicateEvaluator(FIXED)
                .compile(rule(Map.of("severity_at_most", "NORMAL", "idle_for", "PT5M")));

        assertThat(compiled.whereClause()).isEqualTo("severity <= ? AND lasteventtime <= ?");
        assertThat(compiled.params()).hasSize(2);
        assertThat(compiled.params().get(0)).isEqualTo(2);
        // PT5M from FIXED noon → 11:55:00 UTC
        assertThat(((java.sql.Timestamp) compiled.params().get(1)).getTime())
                .isEqualTo(Instant.parse("2026-05-22T11:55:00Z").toEpochMilli());
    }

    @Test
    void ackedAndTicketedBooleansBecomeIsNullClauses() {
        PredicateEvaluator.Compiled c = new PredicateEvaluator(FIXED)
                .compile(rule(Map.of("acked", false, "ticketed", false)));

        assertThat(c.whereClause()).isEqualTo("alarmacktime IS NULL AND tticketstate IS NULL");
        assertThat(c.params()).isEmpty();
    }

    @Test
    void categoryPatternIsParsedButNotEnforcedInSql() {
        PredicateEvaluator.Compiled c = new PredicateEvaluator(FIXED)
                .compile(rule(Map.of("category_pattern", "Prod.*", "idle_for", "P1D")));

        // Iteration order: idleFor THEN categoryPattern (TRUE).
        assertThat(c.whereClause()).isEqualTo("lasteventtime <= ? AND TRUE");
    }

    @Test
    void locationExactMatch() {
        PredicateEvaluator.Compiled c = new PredicateEvaluator(FIXED)
                .compile(rule(Map.of("location", "Default", "severity_at_least", "WARNING")));

        // Iteration order: severityAtLeast THEN location.
        assertThat(c.whereClause()).isEqualTo("severity >= ? AND location = ?");
        assertThat(c.params()).containsExactly(3, "Default");
    }

    @Test
    void emptyPredicateImpossibleViaValidatorButHandledIfReached() {
        RetentionRule r = new RetentionRule("x", "delete",
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        PredicateEvaluator.Compiled c = new PredicateEvaluator(FIXED).compile(r);
        assertThat(c.whereClause()).isEqualTo("FALSE");
        assertThat(c.params()).isEmpty();
    }
}
