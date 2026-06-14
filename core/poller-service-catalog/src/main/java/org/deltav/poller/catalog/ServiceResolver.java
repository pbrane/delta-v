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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Resolves an inventory service name to the catalog definition that serves it (D2).
 *
 * <p>Precedence: an <em>exact</em> definition (one with no {@code pattern}, matched by name
 * equality) beats any pattern; among multiple matching <em>pattern</em> definitions, the
 * first in file order wins. Exact-name matching is <strong>case-insensitive</strong> to
 * mirror the frozen manager's {@code equalsIgnoreCase}, so the FR9 startup check and the
 * translator agree with the engine (resolver parity).
 *
 * <p>Expects a validated catalog. As a defensive measure it skips pattern definitions whose
 * regex does not compile (those are reported as ERRORs by {@link CatalogValidator}); it never
 * throws on a bad pattern.
 *
 * <p><strong>Resolution ignores {@code enabled}</strong> — a disabled definition still
 * matches and can win (exact-beats-pattern, or first-in-file-order among patterns). This is
 * deliberate: the {@code enabled} kill switch is applied by callers per D4 — the translator
 * omits disabled definitions from the synthetic packages, and the FR9 startup check needs to
 * tell "matched but disabled" (exclude from the unscheduled gauge) apart from "no match at
 * all". Callers must therefore consult {@link ServiceDefinition#enabled()} themselves rather
 * than rely on resolution to filter.
 */
public final class ServiceResolver {

    private final Map<String, ServiceDefinition> exactByName;
    private final List<CompiledPattern> patterns; // file order preserved

    public ServiceResolver(final Catalog catalog) {
        // Case-insensitive to match the frozen manager's equalsIgnoreCase exact matching.
        this.exactByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.patterns = new ArrayList<>();
        for (final ServiceDefinition def : catalog.services()) {
            if (def.isPattern()) {
                try {
                    patterns.add(new CompiledPattern(Pattern.compile(def.pattern()), def));
                } catch (final PatternSyntaxException ignored) {
                    // Reported by CatalogValidator; an uncompilable pattern matches nothing.
                }
            } else if (def.name() != null) {
                // Exact definitions: first occurrence wins (duplicates are a validation ERROR).
                exactByName.putIfAbsent(def.name(), def);
            }
        }
    }

    /**
     * Resolves the definition serving the given inventory service name.
     *
     * @param serviceName the inventory service name (e.g. {@code "HTTP-8080"})
     * @return the matching definition, exact first then first-in-file-order pattern, or empty
     */
    public Optional<ServiceDefinition> resolve(final String serviceName) {
        if (serviceName == null) {
            return Optional.empty();
        }
        final ServiceDefinition exact = exactByName.get(serviceName);
        if (exact != null) {
            return Optional.of(exact);
        }
        for (final CompiledPattern cp : patterns) {
            if (cp.pattern.matcher(serviceName).matches()) {
                return Optional.of(cp.definition);
            }
        }
        return Optional.empty();
    }

    private record CompiledPattern(Pattern pattern, ServiceDefinition definition) {
    }
}
