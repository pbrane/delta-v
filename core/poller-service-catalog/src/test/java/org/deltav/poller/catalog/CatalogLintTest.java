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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Story 1.5 acceptance criteria for {@link CatalogLint}: exit 0 clean / 1 errors / 2 usage-IO,
 * collect-all output, and empty-catalog / nested-quantifier as warnings (not failures).
 */
class CatalogLintTest {

    @TempDir
    Path tmp;

    private final ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBuf = new ByteArrayOutputStream();

    @Test
    void cleanCatalogExitsZero() throws IOException {
        final Path file = write("clean.yaml", """
                services:
                  - name: ICMP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                """);
        assertEquals(0, run(file.toString()));
        assertTrue(out().isBlank(), "clean catalog prints no findings, was: " + out());
    }

    @Test
    void defectiveCatalogExitsOneWithCollectAllOutput() throws IOException {
        final Path file = write("bad.yaml", """
                services:
                  - name: DUP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                  - name: DUP
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: -5
                """);
        assertEquals(1, run(file.toString()));
        // collect-all: both the duplicate-name and the bad-interval defects appear in one run.
        assertTrue(out().contains("duplicate service name"), out());
        assertTrue(out().contains("interval must be positive"), out());
    }

    @Test
    void missingFileExitsTwo() {
        assertEquals(2, run(tmp.resolve("nope.yaml").toString()));
    }

    @Test
    void wrongUsageExitsTwo() {
        assertEquals(2, run());
        assertTrue(err().contains("usage"), err());
    }

    @Test
    void emptyCatalogWarnsButExitsZero() throws IOException {
        final Path file = write("empty.yaml", "services: []\n");
        assertEquals(0, run(file.toString()));
        assertTrue(out().contains("catalog is empty"), out());
    }

    @Test
    void anchorsAreReportedAsErrorExitOne() throws IOException {
        final Path file = write("anchors.yaml", """
                services:
                  - name: A
                    monitor: &mon org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                  - name: B
                    monitor: *mon
                    interval: 300000
                """);
        assertEquals(1, run(file.toString()));
        assertTrue(out().contains("anchors"), out());
    }

    @Test
    void nestedQuantifierWarnsButExitsZero() throws IOException {
        final Path file = write("risky.yaml", """
                services:
                  - name: Risky
                    pattern: "(a+)+"
                    monitor: org.opennms.netmgt.poller.monitors.IcmpMonitor
                    interval: 300000
                """);
        assertEquals(0, run(file.toString()));
        assertTrue(out().contains("nested quantifiers"), out());
    }

    private int run(final String... args) {
        return CatalogLint.run(args,
                new PrintStream(outBuf, true, StandardCharsets.UTF_8),
                new PrintStream(errBuf, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return outBuf.toString(StandardCharsets.UTF_8);
    }

    private String err() {
        return errBuf.toString(StandardCharsets.UTF_8);
    }

    private Path write(final String name, final String content) throws IOException {
        final Path file = tmp.resolve(name);
        Files.writeString(file, content);
        return file;
    }
}
