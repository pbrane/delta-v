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
package org.deltav.netmgt.provision.boot;

/**
 * Canonical Micrometer meter names for provisiond domain signals. Names become
 * {@code deltav_provisiond_*} at {@code /actuator/prometheus} after Micrometer's
 * dot-to-underscore flattening.
 *
 * <p>Provisiond emits a known, finite set of UEIs for its lifecycle events.
 * Rather than tag a single counter by UEI (which works but is harder to query),
 * each high-value UEI gets its own dedicated counter so PromQL queries are
 * direct (e.g., {@code rate(deltav_provisiond_imports_successful_total[5m])}).
 */
public final class ProvisiondDomainMetrics {

    private ProvisiondDomainMetrics() {}

    public static final String EVENTS_FORWARDED        = "deltav.provisiond.events.forwarded";
    public static final String IMPORTS_STARTED         = "deltav.provisiond.imports.started";
    public static final String IMPORTS_SUCCESSFUL      = "deltav.provisiond.imports.successful";
    public static final String IMPORTS_FAILED          = "deltav.provisiond.imports.failed";
    public static final String NODES_ADDED             = "deltav.provisiond.nodes.added";
    public static final String NODES_UPDATED           = "deltav.provisiond.nodes.updated";
    public static final String NODES_DELETED           = "deltav.provisiond.nodes.deleted";
}
