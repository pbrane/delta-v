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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One flat poller service definition (FR1).
 *
 * <p>Shape: {@code { name, pattern?, monitor, interval, enabled?=true, parameters }}.
 * {@code pattern} (optional regex) preserves dynamic service families — one definition
 * serving many inventory service names, capture groups feeding the engine's pattern
 * variables. {@code enabled} is a type-level kill switch (distinct from per-entity
 * inventory exclusion). {@code parameters} keys are verbatim monitor parameter names,
 * never translated; values are always strings (metadata DSL chains, block scalars).
 *
 * <p>The compact constructor performs <em>normalization only</em> — never validation.
 * Required-field, interval, pattern-compile, and duplicate checks live in
 * {@link CatalogValidator}, which collects all problems in one pass; record constructors
 * can only throw-first. {@code interval} is an {@link Integer} (not {@code int}) so a
 * missing value surfaces as a validator message rather than a parse-time failure.
 *
 * @param name       the definition's unique identity and synthetic package name
 *                   ({@code catalog-<name>}); always required. For a definition without a
 *                   {@code pattern} it is also the exact inventory service name matched. A
 *                   {@code pattern} is an additional regex matcher, not a replacement for name.
 * @param pattern    optional regex matching a family of inventory service names
 * @param monitor    fully-qualified monitor class name (required)
 * @param interval   poll interval in milliseconds (required, positive)
 * @param enabled    type-level kill switch; defaults {@code true} when omitted
 * @param parameters verbatim monitor parameters (never translated); defaults empty
 */
public record ServiceDefinition(
        String name,
        String pattern,
        String monitor,
        Integer interval,
        Boolean enabled,
        Map<String, String> parameters) {

    public ServiceDefinition {
        enabled = (enabled == null) ? Boolean.TRUE : enabled;
        parameters = (parameters == null)
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
    }

    /** @return {@code true} if this definition matches inventory names by regex rather than exact name. */
    public boolean isPattern() {
        return pattern != null && !pattern.isBlank();
    }
}
