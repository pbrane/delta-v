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
package org.deltav.netmgt.bsm.boot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.opennms.netmgt.bsm.daemon.Bsmd;
import org.opennms.netmgt.bsm.service.AlarmProvider;
import org.opennms.netmgt.bsm.service.BusinessServiceManager;
import org.opennms.netmgt.bsm.service.BusinessServiceStateMachine;
import org.opennms.netmgt.events.api.EventSubscriptionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for the BSMd Spring Boot application.
 *
 * <p>Starts a full Spring Boot context backed by Testcontainers PostgreSQL
 * (with schema.sql). Verifies that JPA DAOs are functional and that the
 * BSMd daemon, state machine, and business service manager beans are
 * properly wired.</p>
 */
@SpringBootTest(classes = BsmdApplication.class)
@Testcontainers
@Import(BsmdApplicationIT.TestConfig.class)
@Transactional
class BsmdApplicationIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("deltav")
            .withUsername("deltav")
            .withPassword("deltav")
            .withInitScript("schema.sql");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("opennms.kafka.bootstrap-servers", () -> "localhost:9092");
    }

    /**
     * Provides mock beans for services that BSMd requires but that are
     * not part of the JPA/DAO layer being tested.
     */
    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        public EventSubscriptionService eventSubscriptionService() {
            return mock(EventSubscriptionService.class);
        }

        @Bean
        public AlarmProvider alarmProvider() {
            return mock(AlarmProvider.class);
        }
    }

    @Autowired
    private Bsmd bsmd;

    @Autowired
    private BusinessServiceStateMachine stateMachine;

    @Autowired
    private BusinessServiceManager manager;

    @Test
    void contextLoads() {
        assertThat(bsmd).isNotNull();
        assertThat(stateMachine).isNotNull();
        assertThat(manager).isNotNull();
    }
}
