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

/**
 * A catalog <em>content</em> defect — malformed YAML, an unknown key, a duplicate key, or an
 * anchor/alias/merge key. Distinct from a plain {@link IOException} (a real read failure), so
 * callers keep the two failure classes from blurring: a content defect is the operator's to
 * fix (lint exit 1; loader → context failure), whereas an I/O failure is environmental (lint
 * exit 2). It extends {@link IOException} so existing {@code throws}/{@code catch} sites are
 * unaffected.
 */
public class CatalogParseException extends IOException {

    public CatalogParseException(final String message) {
        super(message);
    }

    public CatalogParseException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
