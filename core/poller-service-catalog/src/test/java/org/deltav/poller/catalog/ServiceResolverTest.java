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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Story 1.3 acceptance criteria for {@link ServiceResolver}: exact beats pattern, and among
 * patterns the first in file order wins (D2).
 */
class ServiceResolverTest {

    /** AC: a name matching both an exact and a pattern definition resolves to the exact one. */
    @Test
    void exactBeatsPatternEvenWhenPatternIsListedFirst() {
        final ServiceDefinition pattern =
                def("HttpFamily", "^HTTP-(\\d+)$", "a.b.PageSequenceMonitor");
        final ServiceDefinition exact =
                def("HTTP-8080", null, "a.b.HttpMonitor");
        // Pattern deliberately precedes the exact definition in file order.
        final ServiceResolver resolver = new ServiceResolver(new Catalog(List.of(pattern, exact)));

        assertEquals("HTTP-8080", resolver.resolve("HTTP-8080").orElseThrow().name());
    }

    /** AC: among multiple matching patterns, the first in file order wins. */
    @Test
    void firstMatchingPatternInFileOrderWins() {
        final ServiceDefinition first = def("First", "^HTTP-(\\d+)$", "a.b.M1");
        final ServiceDefinition second = def("Second", "^HTTP-9.*$", "a.b.M2");
        final ServiceResolver resolver = new ServiceResolver(new Catalog(List.of(first, second)));

        // "HTTP-9090" matches both patterns; file order puts First ahead of Second.
        assertEquals("First", resolver.resolve("HTTP-9090").orElseThrow().name());
    }

    /** Exact matching is case-insensitive, mirroring the frozen manager's equalsIgnoreCase. */
    @Test
    void exactMatchIsCaseInsensitive() {
        final ServiceResolver resolver = new ServiceResolver(new Catalog(List.of(
                def("HTTP-8080", null, "a.b.HttpMonitor"))));
        assertEquals("HTTP-8080", resolver.resolve("http-8080").orElseThrow().name());
        assertEquals("HTTP-8080", resolver.resolve("HTTP-8080").orElseThrow().name());
    }

    /** A name matching nothing resolves to empty (the FR9 unscheduled path). */
    @Test
    void noMatchResolvesEmpty() {
        final ServiceResolver resolver = new ServiceResolver(new Catalog(List.of(
                def("ICMP", null, "a.b.IcmpMonitor"))));
        assertTrue(resolver.resolve("SNMP").isEmpty());
    }

    private static ServiceDefinition def(final String name, final String pattern, final String monitor) {
        return new ServiceDefinition(name, pattern, monitor, 300000, Boolean.TRUE, Map.of());
    }
}
