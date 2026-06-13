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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * Story 1.2 acceptance criteria for {@link CatalogParser} and the catalog model records.
 */
class CatalogParserTest {

    private final CatalogParser parser = new CatalogParser();

    /** AC: records carry name/pattern/monitor/interval/enabled/parameters with enabled defaulting true. */
    @Test
    void parsesMinimalAndAppliesDefaults() throws IOException {
        final Catalog catalog = parser.parse(fixture("valid-minimal.yaml"));

        assertEquals(1, catalog.services().size());
        final ServiceDefinition icmp = catalog.services().get(0);
        assertEquals("ICMP", icmp.name());
        assertEquals("org.opennms.netmgt.poller.monitors.IcmpMonitor", icmp.monitor());
        assertEquals(300000, icmp.interval());
        assertNull(icmp.pattern(), "pattern omitted -> null");
        assertFalse(icmp.isPattern());
        assertEquals(Boolean.TRUE, icmp.enabled(), "enabled omitted -> defaults true");
        assertTrue(icmp.parameters().isEmpty(), "parameters omitted -> empty map");
    }

    /** AC: an unknown key fails parsing with an error naming the unknown key (FR1/D1 strict mode). */
    @Test
    void unknownKeyFailsNamingTheKey() {
        final String yaml = """
                services:
                  - name: ICMP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    intervall: 30000
                """;
        final IOException ex = assertThrows(IOException.class, () -> parse(yaml));
        assertTrue(ex.getMessage().contains("intervall"),
                "strict-mode error must name the unknown key, was: " + ex.getMessage());
    }

    /** Strictness: a duplicate YAML key fails parsing rather than silently taking last-wins. */
    @Test
    void duplicateKeyFails() {
        final String yaml = """
                services:
                  - name: ICMP
                    name: SNMP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                """;
        final IOException ex = assertThrows(IOException.class, () -> parse(yaml));
        assertTrue(ex.getMessage().toLowerCase().contains("duplicate"),
                "duplicate-key error expected, was: " + ex.getMessage());
    }

    /** AC: metadata DSL chains and a block scalar round-trip verbatim as strings — no key translation. */
    @Test
    void parameterValuesAndKeysRoundTripVerbatim() throws IOException {
        final Catalog catalog = parser.parse(fixture("pattern-family.yaml"));
        assertEquals(2, catalog.services().size());

        final ServiceDefinition http = catalog.services().get(0);
        assertEquals("HTTP-8080", http.name());
        assertEquals("${requisition:port|detector:port|80}", http.parameters().get("port"),
                "metadata DSL chain must survive verbatim, unquoted of its braces");

        final ServiceDefinition seq = catalog.services().get(1);
        assertEquals("^HTTP-(\\d+)$", seq.pattern());
        assertTrue(seq.isPattern());
        assertEquals(Boolean.FALSE, seq.enabled(), "explicit enabled:false honored");
        // Key is verbatim (kebab-case, never camelCased) and the block scalar text is preserved.
        assertTrue(seq.parameters().containsKey("page-sequence"), "parameter key kept verbatim");
        final String pageSequence = seq.parameters().get("page-sequence");
        assertTrue(pageSequence.contains("<page-sequence>")
                        && pageSequence.contains("port=\"8080\""),
                "nested-XML block scalar must round-trip as a string, was: " + pageSequence);
    }

    /**
     * D8 pin: YAML anchors / aliases / merge keys are explicitly rejected (jackson-dataformat-yaml
     * does not resolve them and would otherwise silently bind an alias's name as the value). The
     * parser detects them and fails loudly with a clear message — no silent middle ground.
     */
    @Test
    void yamlAnchorsAreRejectedAsUnsupported() throws IOException {
        final IOException ex = assertThrows(IOException.class, () -> parser.parse(fixture("anchors.yaml")));
        assertTrue(ex.getMessage().contains("anchors"), ex.getMessage());
    }

    /** Even an anchor/alias on a String field (which jackson would silently mis-bind) is rejected. */
    @Test
    void yamlAnchorOnStringFieldIsRejected() {
        final String yaml = """
                services:
                  - name: ICMP
                    monitor: &mon org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                  - name: ICMP2
                    monitor: *mon
                    interval: 300000
                """;
        final IOException ex = assertThrows(IOException.class, () -> parse(yaml));
        assertTrue(ex.getMessage().contains("anchors"), ex.getMessage());
    }

    private Catalog parse(final String yaml) throws IOException {
        try (Reader reader = new StringReader(yaml)) {
            return parser.parse(reader);
        }
    }

    private static Path fixture(final String name) {
        final URL url = CatalogParserTest.class.getResource("/catalogs/" + name);
        if (url == null) {
            throw new IllegalStateException("missing test fixture: catalogs/" + name);
        }
        try {
            return Path.of(url.toURI());
        } catch (final java.net.URISyntaxException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }
}
