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

import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.opennms.core.schema.Migrator;
import org.springframework.context.ApplicationContext;

class DbInitRunnerTest {

    private static DbInitRunner runnerWith(boolean skipVersionCheck) {
        DbInitProperties props = new DbInitProperties(
            "opennms", "opennms", "opennms",                 // databaseName, databaseUser, databasePassword
            "postgres", "postgres",                          // adminUser, adminPassword
            "jdbc:postgresql://localhost:5432/template1",    // adminUrl
            false, false,                                    // createUser, createDatabase
            false, false, false, false,                      // iplike, timescaleDb, vacuum, fullVacuum
            skipVersionCheck);
        return new DbInitRunner(mock(DataSource.class), mock(DataSource.class),
            mock(ApplicationContext.class), props);
    }

    @Test
    void skipVersionCheckTrue_disablesMigratorVersionValidation() {
        Migrator migrator = mock(Migrator.class);

        runnerWith(true).configureMigrator(migrator);

        // The "-Q" escape hatch: skip the migrator's hardcoded version ceiling so
        // PostgreSQL 17.x (CNPG 1.29 default) is not rejected (issue #243).
        verify(migrator).setValidateDatabaseVersion(false);
    }

    @Test
    void skipVersionCheckFalse_leavesMigratorVersionValidationEnabled() {
        Migrator migrator = mock(Migrator.class);

        runnerWith(false).configureMigrator(migrator);

        // Default: the migrator keeps its built-in version validation.
        verify(migrator, never()).setValidateDatabaseVersion(anyBoolean());
    }
}
