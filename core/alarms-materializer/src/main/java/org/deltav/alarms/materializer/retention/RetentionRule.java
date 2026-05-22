/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * A validated retention rule. All predicate fields are optional; absent fields
 * are not matched. Within a rule, present predicates are AND-ed.
 */
public record RetentionRule(
        String id,
        String action,
        Optional<Integer> severityAtMost,
        Optional<Integer> severityAtLeast,
        Optional<Duration> idleFor,
        Optional<Boolean> acked,
        Optional<Boolean> ticketed,
        Optional<String> ueiPattern,
        Optional<String> categoryPattern,
        Optional<String> location) {

    /** Build a rule from a validated raw map (output of YAML loader + JSON-Schema validator). */
    @SuppressWarnings("unchecked")
    public static RetentionRule fromMap(Map<String, Object> raw) {
        String id = (String) raw.get("id");
        String action = (String) raw.get("action");
        Map<String, Object> when = (Map<String, Object>) raw.get("when");
        return new RetentionRule(
                id, action,
                severity(when, "severity_at_most"),
                severity(when, "severity_at_least"),
                Optional.ofNullable((String) when.get("idle_for")).map(Duration::parse),
                Optional.ofNullable((Boolean) when.get("acked")),
                Optional.ofNullable((Boolean) when.get("ticketed")),
                Optional.ofNullable((String) when.get("uei_pattern")),
                Optional.ofNullable((String) when.get("category_pattern")),
                Optional.ofNullable((String) when.get("location"))
        );
    }

    private static Optional<Integer> severity(Map<String, Object> when, String key) {
        String v = (String) when.get(key);
        if (v == null) return Optional.empty();
        return Optional.of(severityNumber(v));
    }

    static int severityNumber(String label) {
        return switch (label) {
            case "INDETERMINATE" -> 0;
            case "CLEARED"       -> 1;
            case "NORMAL"        -> 2;
            case "WARNING"       -> 3;
            case "MINOR"         -> 4;
            case "MAJOR"         -> 5;
            case "CRITICAL"      -> 6;
            default -> throw new IllegalArgumentException("Unknown severity: " + label);
        };
    }
}
