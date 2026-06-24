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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies issue #243: db-init must initialize a schema on PostgreSQL 17.x — the
 * CloudNativePG 1.29 default — which the schema migrator otherwise rejects with its
 * hardcoded {@code < 17.0} version ceiling. With {@code skip-version-check=true}
 * (OPENNMS_DBINIT_SKIP_VERSION_CHECK) the ceiling is bypassed and migration succeeds.
 *
 * <p>Without the flag this same container would fail context startup with
 * {@code MigrationException: Unsupported database version "17.x"} — which is exactly
 * the production failure #243 reports.
 */
@SpringBootTest
@Testcontainers
class DbInitPostgres17IntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17")
                    .withDatabaseName("template1")
                    .withUsername("postgres")
                    .withPassword("postgres");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("opennms.dbinit.admin-url", postgres::getJdbcUrl);
        registry.add("opennms.dbinit.admin-user", postgres::getUsername);
        registry.add("opennms.dbinit.admin-password", postgres::getPassword);
        registry.add("opennms.dbinit.database-name", () -> "opennms");
        registry.add("opennms.dbinit.database-user", () -> "opennms");
        registry.add("opennms.dbinit.database-password", () -> "opennms");
        // The fix under test: opt past the migrator's PostgreSQL-version ceiling.
        registry.add("opennms.dbinit.skip-version-check", () -> "true");
    }

    @Autowired
    private DataSource dataSource;

    @Test
    void migrationSucceedsOnPostgres17() throws Exception {
        // Reaching this test at all means DbInitRunner completed setupDatabase against
        // PostgreSQL 17 during context startup (it would have thrown otherwise).
        try (var conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, "public", "alarms", null)) {
            assertThat(rs.next())
                    .as("alarms table should exist after migration on PostgreSQL 17")
                    .isTrue();
        }
    }
}
