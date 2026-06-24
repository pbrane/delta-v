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
import static org.assertj.core.api.Assertions.assertThatNoException;

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

@SpringBootTest
@Testcontainers
class DbInitIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16")
                    .withDatabaseName("template1")
                    .withUsername("postgres")
                    .withPassword("postgres");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("opennms.dbinit.admin-url", postgres::getJdbcUrl);
        registry.add("opennms.dbinit.admin-user", postgres::getUsername);
        registry.add("opennms.dbinit.admin-password", postgres::getPassword);
        registry.add("opennms.dbinit.database-name", () -> "deltav");
        registry.add("opennms.dbinit.database-user", () -> "deltav");
        registry.add("opennms.dbinit.database-password", () -> "deltav");
    }

    @Autowired
    private DataSource dataSource;

    @Autowired
    private DbInitRunner runner;

    @Test
    void migrationCreatesAlarmTable() throws Exception {
        try (var conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, "public", "alarms", null)) {
            assertThat(rs.next())
                    .as("alarms table should exist after migration")
                    .isTrue();
        }
    }

    @Test
    void migrationCreatesNodeTable() throws Exception {
        try (var conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, "public", "node", null)) {
            assertThat(rs.next())
                    .as("node table should exist after migration")
                    .isTrue();
        }
    }

    @Test
    void eventsTableDoesNotExist() throws Exception {
        try (var conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, "public", "events", null)) {
            assertThat(rs.next())
                    .as("events table should NOT exist (dropped by 36.0.0)")
                    .isFalse();
        }
    }

    @Test
    void migrationIsIdempotent() {
        assertThatNoException().isThrownBy(() -> runner.run());
    }
}
