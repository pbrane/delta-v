/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.upsert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

class ServiceTypeResolverTest {

    @Test
    void cachesLookupAfterFirstHit() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Integer.class), eq("ICMP"))).thenReturn(7);

        ServiceTypeResolver resolver = new ServiceTypeResolver(jdbc);

        assertThat(resolver.resolve("ICMP")).isEqualTo(7);
        assertThat(resolver.resolve("ICMP")).isEqualTo(7);
        assertThat(resolver.resolve("ICMP")).isEqualTo(7);

        verify(jdbc, times(1)).queryForObject(anyString(), eq(Integer.class), eq("ICMP"));
        verifyNoMoreInteractions(jdbc);
    }

    @Test
    void missingServiceReturnsNullAndCachesTheMiss() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Integer.class), eq("UNKNOWN")))
                .thenThrow(new EmptyResultDataAccessException(1));

        ServiceTypeResolver resolver = new ServiceTypeResolver(jdbc);

        assertThat(resolver.resolve("UNKNOWN")).isNull();
        assertThat(resolver.resolve("UNKNOWN")).isNull();   // cached miss

        verify(jdbc, times(1)).queryForObject(anyString(), eq(Integer.class), eq("UNKNOWN"));
    }

    @Test
    void nullOrEmptyNameReturnsNullWithoutQuerying() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ServiceTypeResolver resolver = new ServiceTypeResolver(jdbc);

        assertThat(resolver.resolve(null)).isNull();
        assertThat(resolver.resolve("")).isNull();

        verifyNoMoreInteractions(jdbc);
    }
}
