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
package org.deltav.core.dbinit;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "opennms.dbinit")
public record DbInitProperties(
    String databaseName,
    String databaseUser,
    String databasePassword,
    String adminUser,
    String adminPassword,
    String adminUrl,
    boolean createUser,
    boolean createDatabase,
    boolean iplike,
    boolean timescaleDb,
    boolean vacuum,
    boolean fullVacuum,
    // When true, skip the schema migrator's database-version check (the "-Q" escape
    // hatch). Lets deployments opt into PostgreSQL versions above the migrator's
    // hardcoded ceiling — e.g. PostgreSQL 17, the CloudNativePG 1.29 default — once
    // they've accepted the compatibility risk. Bound from OPENNMS_DBINIT_SKIP_VERSION_CHECK.
    boolean skipVersionCheck
) {}
