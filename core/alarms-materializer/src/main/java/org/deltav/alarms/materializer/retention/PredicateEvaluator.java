/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Compiles a {@link RetentionRule} into a SQL {@code WHERE} fragment + parameter
 * list. Predicates within a rule are AND-ed. {@code category_pattern} is parsed
 * but not enforced at the SQL layer in v1.3 (no category join) — logged once
 * per rule.
 */
@Component
public class PredicateEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(PredicateEvaluator.class);

    private final Clock clock;

    public PredicateEvaluator(Clock clock) {
        this.clock = clock;
    }

    /** Compiled SQL fragment + parameters for a rule. */
    public record Compiled(String whereClause, List<Object> params) {}

    public Compiled compile(RetentionRule rule) {
        List<String> clauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        rule.severityAtMost().ifPresent(v -> { clauses.add("severity <= ?"); params.add(v); });
        rule.severityAtLeast().ifPresent(v -> { clauses.add("severity >= ?"); params.add(v); });
        rule.idleFor().ifPresent(d -> {
            Instant cutoff = clock.instant().minus(d);
            clauses.add("lasteventtime <= ?");
            params.add(Timestamp.from(cutoff));
        });
        rule.acked().ifPresent(b -> clauses.add(b ? "alarmacktime IS NOT NULL" : "alarmacktime IS NULL"));
        rule.ticketed().ifPresent(b -> clauses.add(b ? "tticketstate IS NOT NULL" : "tticketstate IS NULL"));
        rule.ueiPattern().ifPresent(p -> { clauses.add("eventuei ~ ?"); params.add(p); });
        rule.categoryPattern().ifPresent(p -> {
            LOG.warn("Retention rule {}: category_pattern is accepted but not enforced at SQL layer in v1.3", rule.id());
            clauses.add("TRUE");
        });
        rule.location().ifPresent(l -> { clauses.add("location = ?"); params.add(l); });

        String where = clauses.isEmpty() ? "FALSE" : String.join(" AND ", clauses);
        return new Compiled(where, params);
    }
}
