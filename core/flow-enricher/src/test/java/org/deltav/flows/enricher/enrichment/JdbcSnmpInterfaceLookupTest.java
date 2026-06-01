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
package org.deltav.flows.enricher.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class JdbcSnmpInterfaceLookupTest {

    @Test
    void returnsIfNameAndCachesIt() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class), eq(5), eq(3)))
                .thenReturn(List.of("Gi0/3"));

        JdbcSnmpInterfaceLookup lookup = new JdbcSnmpInterfaceLookup(jdbc, Duration.ofMinutes(5));

        assertThat(lookup.lookupIfName(5, 3)).isEqualTo("Gi0/3");
        assertThat(lookup.lookupIfName(5, 3)).isEqualTo("Gi0/3"); // second call cached
        verify(jdbc, times(1)).queryForList(anyString(), eq(String.class), eq(5), eq(3));
    }

    @Test
    void returnsNullWhenInterfaceMissing() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class), eq(5), eq(99)))
                .thenReturn(List.of());

        JdbcSnmpInterfaceLookup lookup = new JdbcSnmpInterfaceLookup(jdbc, Duration.ofMinutes(5));
        assertThat(lookup.lookupIfName(5, 99)).isNull();
    }

    @Test
    void skipsQueryForUnresolvedNode() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        JdbcSnmpInterfaceLookup lookup = new JdbcSnmpInterfaceLookup(jdbc, Duration.ofMinutes(5));
        assertThat(lookup.lookupIfName(0, 3)).isNull();
        verifyNoInteractions(jdbc);
    }
}
