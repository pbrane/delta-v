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

/**
 * One validation finding against a catalog. The {@link CatalogValidator} returns these in
 * a collect-all pass; the lint CLI and the daemon startup check render them one per line.
 *
 * <p>Render format (operator-facing contract): {@code LEVEL: <file>: services[<index>] (<name>): <message>}.
 * The {@code <file>} label is supplied at render time because the validator works on a
 * parsed model and does not know the source path.
 *
 * @param level       severity (ERROR fails lint; WARN does not)
 * @param index       zero-based position of the offending service in the catalog
 * @param serviceName the service definition's {@code name} (may be null/blank if that is the defect)
 * @param message     the human-readable problem description
 */
public record ValidationMessage(Level level, int index, String serviceName, String message) {

    /** Severity levels. Exactly two — anything that must block the build is an ERROR. */
    public enum Level { ERROR, WARN }

    /** @return {@code true} if this finding is an ERROR (build-blocking). */
    public boolean isError() {
        return level == Level.ERROR;
    }

    /**
     * Renders this finding in the operator-facing single-line format.
     *
     * @param fileLabel the source file label to embed (e.g. {@code poller-services.yaml})
     * @return {@code LEVEL: <file>: services[<index>] (<name>): <message>}
     */
    public String format(final String fileLabel) {
        return level + ": " + fileLabel + ": services[" + index + "] ("
                + (serviceName == null ? "" : serviceName) + "): " + message;
    }
}
