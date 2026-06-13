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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.deltav.poller.catalog.ValidationMessage.Level;
import org.junit.jupiter.api.Test;

/**
 * Story 1.3 acceptance criteria for {@link CatalogValidator}: collect-all (never first-fail)
 * and the single-line message format.
 */
class CatalogValidatorTest {

    private final CatalogParser parser = new CatalogParser();
    private final CatalogValidator validator = new CatalogValidator();

    /** AC: a catalog with N distinct defects yields all N messages in one pass — never first-fail. */
    @Test
    void collectsAllDefectsInOnePass() throws IOException {
        final Catalog catalog = parser.parse(load("""
                services:
                  - name: DUP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                  - name: DUP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                  - name: BadInterval
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: -5
                  - name: BadPattern
                    pattern: "(unclosed"
                    monitor: org.opennms.netmgt.poller.monitors.HttpMonitor
                    interval: 300000
                """));

        final List<ValidationMessage> messages = validator.validate(catalog);
        final List<ValidationMessage> errors = messages.stream().filter(ValidationMessage::isError).toList();

        // 2 duplicate-name + 1 bad-interval + 1 bad-pattern = 4 distinct ERRORs.
        assertEquals(4, errors.size(), "all four defects must be collected, not first-failed: " + render(errors));
        assertEquals(2, errors.stream().filter(m -> m.message().contains("duplicate service name")).count());
        assertTrue(errors.stream().anyMatch(m -> m.message().contains("interval must be positive")));
        assertTrue(errors.stream().anyMatch(m -> m.message().contains("pattern does not compile")));
    }

    /** AC: messages render as {@code LEVEL: <file>: services[<index>] (<name>): <message>}. */
    @Test
    void rendersInTheSingleLineFormat() throws IOException {
        final Catalog catalog = parser.parse(load("""
                services:
                  - name: ICMP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                  - name: BadInterval
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: -5
                """));

        final ValidationMessage badInterval = validator.validate(catalog).stream()
                .filter(m -> "BadInterval".equals(m.serviceName()))
                .findFirst().orElseThrow();

        assertEquals("ERROR: poller-services.yaml: services[1] (BadInterval): interval must be positive, was -5",
                badInterval.format("poller-services.yaml"));
    }

    /** AC: an interval below the 5s floor yields a WARN-level message (not an ERROR). */
    @Test
    void intervalBelowFloorIsWarn() {
        final Catalog catalog = new Catalog(List.of(
                new ServiceDefinition("Fast", null, "a.b.FastMonitor", 1000, null, Map.of())));

        final List<ValidationMessage> messages = validator.validate(catalog);

        assertEquals(1, messages.size());
        assertEquals(Level.WARN, messages.get(0).level());
        assertTrue(messages.get(0).message().contains("below the 5000ms floor"));
    }

    /** A clean catalog produces no findings. */
    @Test
    void cleanCatalogHasNoMessages() throws IOException {
        final Catalog catalog = parser.parse(CatalogParserTestFixtures.minimal());
        assertTrue(validator.validate(catalog).isEmpty());
    }

    private static Reader load(final String yaml) {
        return new StringReader(yaml);
    }

    private static String render(final List<ValidationMessage> messages) {
        return messages.stream().map(m -> m.format("test.yaml")).reduce("", (a, b) -> a + "\n" + b);
    }

    /** Tiny inline fixture helper to avoid a redundant resource file for the clean case. */
    private static final class CatalogParserTestFixtures {
        static Reader minimal() {
            return new StringReader("""
                    services:
                      - name: ICMP
                        monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                        interval: 300000
                    """);
        }
    }
}
