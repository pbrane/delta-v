/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.upsert;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class AlarmUpsertWriterTest {

    @Test
    void executesUpsertSqlWithProjectedRow() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ServiceTypeResolver resolver = mock(ServiceTypeResolver.class);
        when(resolver.resolve(eq("ICMP"))).thenReturn(7);

        AlarmUpsertWriter writer = new AlarmUpsertWriter(jdbc,
                new AlarmStateProjector(resolver::resolve));

        AlarmState alarm = AlarmState.newBuilder()
                .setReductionKey("rk").setSeverity(AlarmState.Severity.MAJOR)
                .setUei("uei/x").setNodeId(42).setLocation("Default")
                .setFirstEventTimeMs(100).setLastEventTimeMs(200)
                .setCount(1).setServiceName("ICMP").build();

        writer.upsert(alarm);

        // Column order is locked in Task 5's projector. Verify the 21 args go through verbatim.
        verify(jdbc).update(eq(AlarmUpsertWriter.SQL),
                eq("uei/x"), eq(42), eq(null), eq(7), eq("rk"),         // 0..4
                eq(1), eq(1), eq(AlarmState.Severity.MAJOR.getNumber()),// 5..7
                any(), any(),                                            // 8 firsteventtime, 9 lasteventtime
                eq(null), eq(null),                                      // 10 alarmacktime, 11 alarmackuser
                eq(""), eq(""), eq(null),                                // 12 description, 13 logmsg, 14 ifindex
                eq("Default"),                                           // 15 location
                eq("uei/x"),                                             // 16 event_uei
                eq(AlarmState.Severity.MAJOR.getNumber()),               // 17 event_severity
                any(),                                                   // 18 event_timestamp
                eq(42L),                                                 // 19 event_node_id
                eq(""));                                                 // 20 event_log_msg
    }
}
