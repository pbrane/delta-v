/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.upsert;

import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AlarmStateProjectorTest {

    @Test
    void projectsFullyPopulatedAlarm() {
        AlarmState alarm = AlarmState.newBuilder()
                .setAlarmId(7)             // ignored — PG sequence assigns
                .setReductionKey("uei.opennms.org/nodes/nodeDown::7")
                .setSeverity(AlarmState.Severity.MAJOR)
                .setUei("uei.opennms.org/nodes/nodeDown")
                .setNodeId(1042)
                .setLocation("Default")
                .setFirstEventTimeMs(1_700_000_000_000L)
                .setLastEventTimeMs(1_700_000_001_000L)
                .setAckTimeMs(1_700_000_002_000L)
                .setAckUser("dhustace")
                .setCount(3)
                .setDescription("Node is down")
                .setLogMessage("Node 1042 down")
                .setIpAddress("10.1.1.42")
                .setServiceName("ICMP")
                .setIfIndex(5)
                .build();

        AlarmStateProjector projector = new AlarmStateProjector(s -> 1);   // service-name → serviceid stub
        Object[] row = projector.project(alarm);

        assertThat(row).containsExactly(
                "uei.opennms.org/nodes/nodeDown",                     // 0 eventuei
                1042,                                                  // 1 nodeid
                "10.1.1.42",                                           // 2 ipaddr
                1,                                                     // 3 serviceid (from stub)
                "uei.opennms.org/nodes/nodeDown::7",                  // 4 reductionkey
                1,                                                     // 5 alarmtype (hard-coded)
                3,                                                     // 6 counter
                AlarmState.Severity.MAJOR.getNumber(),                 // 7 severity (enum number)
                new java.sql.Timestamp(1_700_000_000_000L),            // 8 firsteventtime
                new java.sql.Timestamp(1_700_000_001_000L),            // 9 lasteventtime
                new java.sql.Timestamp(1_700_000_002_000L),            // 10 alarmacktime
                "dhustace",                                            // 11 alarmackuser
                "Node is down",                                        // 12 description
                "Node 1042 down",                                      // 13 logmsg
                5,                                                     // 14 ifindex
                "Default",                                             // 15 location
                "uei.opennms.org/nodes/nodeDown",                      // 16 event_uei
                AlarmState.Severity.MAJOR.getNumber(),                 // 17 event_severity
                new java.sql.Timestamp(1_700_000_001_000L),            // 18 event_timestamp
                1042L,                                                 // 19 event_node_id (Long)
                "Node 1042 down"                                       // 20 event_log_msg
        );
    }

    @Test
    void absentFieldsBecomeNull() {
        AlarmState alarm = AlarmState.newBuilder()
                .setReductionKey("rk")
                .setSeverity(AlarmState.Severity.WARNING)
                .setUei("uei/x")
                .setLocation("Default")
                .setFirstEventTimeMs(1L)
                .setLastEventTimeMs(2L)
                .setCount(1)
                .build();

        AlarmStateProjector projector = new AlarmStateProjector(s -> null);
        Object[] row = projector.project(alarm);

        // 21-element row; verify the NULL slots:
        assertThat(row[1]).isNull();   // nodeid
        assertThat(row[2]).isNull();   // ipaddr
        assertThat(row[3]).isNull();   // serviceid
        assertThat(row[10]).isNull();  // alarmacktime
        assertThat(row[11]).isNull();  // alarmackuser (empty → null)
        assertThat(row[14]).isNull();  // ifindex
        assertThat(row[19]).isNull();  // event_node_id (mirrors nodeid)
    }

    @Test
    void servicelessAlarmDoesNotInvokeResolver() {
        boolean[] called = new boolean[]{false};
        AlarmStateProjector projector = new AlarmStateProjector(s -> {
            called[0] = true;
            return 999;
        });
        AlarmState alarm = AlarmState.newBuilder()
                .setReductionKey("rk").setSeverity(AlarmState.Severity.WARNING)
                .setUei("uei/x").setLocation("Default")
                .setFirstEventTimeMs(1L).setLastEventTimeMs(2L).build();

        projector.project(alarm);

        assertThat(called[0]).isFalse();
    }
}
