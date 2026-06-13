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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Build-time catalog lint (FR10 / D5). A zero-framework {@code main()} that parses and
 * validates a catalog and reports every defect in one pass, used both as a Dockerfile build
 * stage (after the overlay COPY — a malformed catalog can never reach a runnable image) and
 * locally via {@code make lint-catalog}.
 *
 * <p>Exit codes: {@code 0} = clean (warnings allowed); {@code 1} = errors found (or a
 * malformed/strict-violating catalog); {@code 2} = usage or I/O failure (missing/unreadable
 * file, wrong arguments). An empty catalog and nested-quantifier patterns are warnings, not
 * failures (D5).
 */
public final class CatalogLint {

    private CatalogLint() {
    }

    public static void main(final String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * Runs the lint. Separated from {@link #main(String[])} (which calls {@link System#exit})
     * so tests can assert the exit code and captured output.
     *
     * @param args single element: the catalog file path
     * @param out  report stream (findings)
     * @param err  diagnostics stream (usage / I/O errors)
     * @return process exit code (0 clean, 1 errors, 2 usage/IO)
     */
    static int run(final String[] args, final PrintStream out, final PrintStream err) {
        if (args.length != 1) {
            err.println("usage: CatalogLint <poller-services.yaml>");
            return 2;
        }

        final Path path = Path.of(args[0]);
        if (!Files.isRegularFile(path)) {
            err.println("ERROR: file not found: " + path);
            return 2;
        }
        if (!Files.isReadable(path)) {
            err.println("ERROR: file not readable: " + path);
            return 2;
        }

        final String fileLabel = path.getFileName().toString();
        final Catalog catalog;
        try {
            catalog = new CatalogParser().parse(path);
        } catch (final CatalogParseException e) {
            // Catalog content defect (malformed YAML, unknown key, anchors) — operator-fixable.
            out.println("ERROR: " + fileLabel + ": " + e.getMessage());
            return 1;
        } catch (final IOException e) {
            // A real read failure — environmental, not a catalog defect.
            err.println("ERROR: cannot read " + path + ": " + e.getMessage());
            return 2;
        }

        if (catalog.services().isEmpty()) {
            out.println("WARN: " + fileLabel + ": catalog is empty (no services defined)");
        }

        boolean hasError = false;
        final List<ValidationMessage> messages = new CatalogValidator().validate(catalog);
        for (final ValidationMessage message : messages) {
            out.println(message.format(fileLabel));
            hasError |= message.isError();
        }

        return hasError ? 1 : 0;
    }
}
