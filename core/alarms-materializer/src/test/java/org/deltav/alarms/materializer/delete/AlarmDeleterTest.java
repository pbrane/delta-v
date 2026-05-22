/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.delete;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class AlarmDeleterTest {

    @Test
    void issuesDeleteKeyedByReductionKey() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(eq(AlarmDeleter.SQL), eq("rk"))).thenReturn(1);

        new AlarmDeleter(jdbc).delete("rk");

        verify(jdbc).update(eq(AlarmDeleter.SQL), eq("rk"));
    }

    @Test
    void zeroRowsAffectedIsBenign() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(eq(AlarmDeleter.SQL), eq("rk"))).thenReturn(0);

        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> new AlarmDeleter(jdbc).delete("rk"));
    }
}
