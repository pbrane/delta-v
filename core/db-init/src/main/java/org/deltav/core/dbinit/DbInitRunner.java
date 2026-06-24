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

import javax.sql.DataSource;

import org.opennms.core.schema.Migrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class DbInitRunner implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(DbInitRunner.class);

    private final DataSource adminDataSource;
    private final DataSource dataSource;
    private final ApplicationContext context;
    private final DbInitProperties properties;

    public DbInitRunner(@Qualifier("adminDataSource") DataSource adminDataSource,
                        @Qualifier("dataSource") DataSource dataSource,
                        ApplicationContext context,
                        DbInitProperties properties) {
        this.adminDataSource = adminDataSource;
        this.dataSource = dataSource;
        this.context = context;
        this.properties = properties;
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Starting database initialization...");

        var migrator = new Migrator();
        configureMigrator(migrator);

        migrator.setupDatabase(
            true,
            properties.vacuum(),
            properties.fullVacuum(),
            properties.iplike(),
            properties.timescaleDb()
        );

        LOG.info("Database initialization complete.");
    }

    /**
     * Configures the migrator from the bound properties. Package-private so the
     * version-check wiring can be unit-tested without a database.
     */
    void configureMigrator(Migrator migrator) {
        migrator.setAdminDataSource(adminDataSource);
        migrator.setDataSource(dataSource);
        migrator.setApplicationContext(context);
        migrator.setDatabaseName(properties.databaseName());
        migrator.setDatabaseUser(properties.databaseUser());
        migrator.setDatabasePassword(properties.databasePassword());
        migrator.setAdminUser(properties.adminUser());
        migrator.setAdminPassword(properties.adminPassword());
        migrator.setCreateUser(properties.createUser());
        migrator.setCreateDatabase(properties.createDatabase());

        if (properties.skipVersionCheck()) {
            LOG.warn("OPENNMS_DBINIT_SKIP_VERSION_CHECK=true: skipping the schema migrator's "
                + "database-version check. The target PostgreSQL version will NOT be validated "
                + "against the migrator's supported range (e.g. this allows PostgreSQL 17.x).");
            migrator.setValidateDatabaseVersion(false);
        }
    }
}
