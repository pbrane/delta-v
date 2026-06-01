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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

class JdbcNodeInfoLookupTest {

    private static final JdbcNodeInfoLookup.NodeInfo SAMPLE =
            new JdbcNodeInfoLookup.NodeInfo(5, "delta-v", "router-1", "Default", "router-1-label");

    @Test
    void lookupByIpAddressReturnsNodeInfo() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(contains("ipaddr"), any(RowMapper.class), eq("192.168.1.1")))
                .thenReturn(List.of(SAMPLE));

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));

        JdbcNodeInfoLookup.NodeInfo result = lookup.lookupByIpAddress("192.168.1.1");

        assertThat(result).isNotNull();
        assertThat(result.nodeId()).isEqualTo(5);
        assertThat(result.foreignSource()).isEqualTo("delta-v");
        assertThat(result.foreignId()).isEqualTo("router-1");
        assertThat(result.location()).isEqualTo("Default");
    }

    @Test
    void lookupByIpAddressReturnsNullWhenNotFound() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), anyString())).thenReturn(List.of());

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));

        assertThat(lookup.lookupByIpAddress("1.2.3.4")).isNull();
    }

    @Test
    void lookupByIpAddressReturnsNullWhenJdbcThrows() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), anyString()))
                .thenThrow(new EmptyResultDataAccessException(1));

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));

        assertThat(lookup.lookupByIpAddress("1.2.3.4")).isNull();
    }

    @Test
    void lookupByIpAddressIsCached() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(contains("ipaddr"), any(RowMapper.class), eq("192.168.1.1")))
                .thenReturn(List.of(SAMPLE));

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));

        lookup.lookupByIpAddress("192.168.1.1");
        lookup.lookupByIpAddress("192.168.1.1");
        lookup.lookupByIpAddress("192.168.1.1");

        verify(jdbc, times(1)).query(anyString(), any(RowMapper.class), anyString());
    }

    @Test
    void lookupByIpAddressNegativeResultIsAlsoCached() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), anyString())).thenReturn(List.of());

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));

        assertThat(lookup.lookupByIpAddress("1.2.3.4")).isNull();
        assertThat(lookup.lookupByIpAddress("1.2.3.4")).isNull();

        // Cache should suppress the second DB call even though the first returned null.
        verify(jdbc, times(1)).query(anyString(), any(RowMapper.class), anyString());
    }

    @Test
    void lookupByNodeIdReturnsNodeInfo() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(contains("nodeid"), any(RowMapper.class), eq(5))).thenReturn(List.of(SAMPLE));

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));

        assertThat(lookup.lookupByNodeId(5)).isEqualTo(SAMPLE);
    }

    @Test
    void lookupByNodeIdIsCached() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(contains("nodeid"), any(RowMapper.class), anyInt())).thenReturn(List.of(SAMPLE));

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));

        lookup.lookupByNodeId(5);
        lookup.lookupByNodeId(5);
        lookup.lookupByNodeId(5);

        verify(jdbc, times(1)).query(anyString(), any(RowMapper.class), any(Object[].class));
    }

    @Test
    void lookupByNodeIdCarriesNodeLabel() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        JdbcNodeInfoLookup.NodeInfo row =
                new JdbcNodeInfoLookup.NodeInfo(7, "nl6", "dev-7", "nl6-lab", "cisco-7");
        when(jdbc.query(contains("nodeid"), any(RowMapper.class), eq(7)))
                .thenReturn(List.of(row));

        JdbcNodeInfoLookup lookup = new JdbcNodeInfoLookup(jdbc, Duration.ofMinutes(5));
        JdbcNodeInfoLookup.NodeInfo result = lookup.lookupByNodeId(7);

        assertThat(result).isNotNull();
        assertThat(result.nodeLabel()).isEqualTo("cisco-7");
    }
}
