/*
 * Copyright (C) 2026 BeaconStrategists, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.deltav.poller.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.deltav.poller.catalog.ValidationMessage.Level;

/**
 * Collect-all-then-report validator for the flat catalog (FR10 / D1 / D2).
 *
 * <p>Never fail-first: every defect in the catalog is reported in one pass so an operator
 * fixes a broken catalog in a single iteration. Structural parse failures (bad YAML,
 * unknown keys) are handled earlier and separately by {@link CatalogParser}; this validator
 * only sees a successfully-parsed model.
 *
 * <p>Checks, per service: {@code name} required; {@code monitor} required and a well-formed
 * fully-qualified class name; {@code interval} required and positive (WARN below the 5s
 * floor); {@code pattern} (if present) compiles (WARN on nested quantifiers — a best-effort
 * catastrophic-backtracking heuristic). Cross-service: duplicate {@code name}s and identical
 * {@code pattern}s are rejected (D2).
 */
public final class CatalogValidator {

    /** Below this interval (ms) a service hot-loops; warn but do not fail (D1). */
    static final int INTERVAL_FLOOR_MS = 5_000;

    /** Dotted Java identifier with at least one dot (monitor classes are always packaged). */
    private static final Pattern FQCN =
            Pattern.compile("^[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+$");

    /** Best-effort: a group that contains a quantifier and is itself quantified, e.g. {@code (a+)+}. */
    private static final Pattern NESTED_QUANTIFIER =
            Pattern.compile("\\([^()]*[*+][^()]*\\)[*+]");

    /**
     * Validates the catalog and returns all findings (collect-all). An empty list means no
     * ERROR or WARN was raised.
     *
     * @param catalog the parsed catalog
     * @return all validation messages, in a deterministic order
     */
    public List<ValidationMessage> validate(final Catalog catalog) {
        final List<ValidationMessage> messages = new ArrayList<>();
        final List<ServiceDefinition> services = catalog.services();

        // Per-service checks.
        for (int i = 0; i < services.size(); i++) {
            validateService(i, services.get(i), messages);
        }

        // Cross-service uniqueness (D2).
        checkDuplicateNames(services, messages);
        checkIdenticalPatterns(services, messages);

        return messages;
    }

    private void validateService(final int index, final ServiceDefinition def,
                                 final List<ValidationMessage> out) {
        if (isBlank(def.name())) {
            out.add(error(index, def.name(), "name is required"));
        }

        if (isBlank(def.monitor())) {
            out.add(error(index, def.name(), "monitor is required"));
        } else if (!FQCN.matcher(def.monitor()).matches()) {
            out.add(error(index, def.name(),
                    "monitor is not a well-formed class name: " + def.monitor()));
        }

        final Integer interval = def.interval();
        if (interval == null) {
            out.add(error(index, def.name(), "interval is required (milliseconds)"));
        } else if (interval <= 0) {
            out.add(error(index, def.name(), "interval must be positive, was " + interval));
        } else if (interval < INTERVAL_FLOOR_MS) {
            out.add(warn(index, def.name(),
                    "interval " + interval + "ms is below the " + INTERVAL_FLOOR_MS + "ms floor"));
        }

        if (def.isPattern()) {
            try {
                Pattern.compile(def.pattern());
                if (NESTED_QUANTIFIER.matcher(def.pattern()).find()) {
                    out.add(warn(index, def.name(),
                            "pattern has nested quantifiers (possible catastrophic backtracking): "
                                    + def.pattern()));
                }
            } catch (final PatternSyntaxException e) {
                out.add(error(index, def.name(),
                        "pattern does not compile: " + e.getDescription()));
            }
        }
    }

    private void checkDuplicateNames(final List<ServiceDefinition> services,
                                     final List<ValidationMessage> out) {
        final Map<String, List<Integer>> byName = new LinkedHashMap<>();
        for (int i = 0; i < services.size(); i++) {
            final String name = services.get(i).name();
            if (!isBlank(name)) {
                byName.computeIfAbsent(name, k -> new ArrayList<>()).add(i);
            }
        }
        for (final Map.Entry<String, List<Integer>> e : byName.entrySet()) {
            if (e.getValue().size() > 1) {
                for (final int index : e.getValue()) {
                    out.add(error(index, e.getKey(), "duplicate service name '" + e.getKey() + "'"));
                }
            }
        }
    }

    private void checkIdenticalPatterns(final List<ServiceDefinition> services,
                                        final List<ValidationMessage> out) {
        final Map<String, List<Integer>> byPattern = new LinkedHashMap<>();
        for (int i = 0; i < services.size(); i++) {
            final ServiceDefinition def = services.get(i);
            if (def.isPattern()) {
                byPattern.computeIfAbsent(def.pattern(), k -> new ArrayList<>()).add(i);
            }
        }
        for (final Map.Entry<String, List<Integer>> e : byPattern.entrySet()) {
            if (e.getValue().size() > 1) {
                for (final int index : e.getValue()) {
                    out.add(error(index, services.get(index).name(),
                            "identical pattern '" + e.getKey() + "'"));
                }
            }
        }
    }

    private static ValidationMessage error(final int index, final String name, final String msg) {
        return new ValidationMessage(Level.ERROR, index, name, msg);
    }

    private static ValidationMessage warn(final int index, final String name, final String msg) {
        return new ValidationMessage(Level.WARN, index, name, msg);
    }

    private static boolean isBlank(final String s) {
        return s == null || s.isBlank();
    }
}
