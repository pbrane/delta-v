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

import java.util.List;

/**
 * The parsed flat poller service catalog: an ordered list of service definitions.
 *
 * <p>Order is significant — among multiple matching pattern definitions, first in file
 * order wins (D2). The compact constructor normalizes a missing list to empty (an empty
 * catalog is a lint warning, not a failure, per D5); it never validates.
 *
 * @param services the service definitions, in file order
 */
public record Catalog(List<ServiceDefinition> services) {

    public Catalog {
        services = (services == null) ? List.of() : List.copyOf(services);
    }
}
