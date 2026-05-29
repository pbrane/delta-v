/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.upsert;

import java.sql.Timestamp;
import java.util.function.Function;

import org.deltav.alarms.proto.AlarmState;

/**
 * Pure function: maps an {@link AlarmState} proto to an ordered parameter array
 * matching the INSERT column list in {@code AlarmUpsertWriter}. No I/O; the
 * service-name -> serviceid lookup is injected via a {@link Function}.
 *
 * <p>Column order (MUST stay in lock-step with {@code AlarmUpsertWriter}):
 * {@code eventuei, nodeid, ipaddr, serviceid, reductionkey, alarmtype,
 * counter, severity, firsteventtime, lasteventtime, alarmacktime,
 * alarmackuser, description, logmsg, ifindex, location,
 * event_uei, event_severity, event_timestamp, event_node_id, event_log_msg}.
 * {@code alarmid} is omitted — PG's sequence assigns it via DEFAULT.
 * {@code event_tsid}, {@code event_source}, {@code last_event_data} are also
 * omitted — they default to NULL (no proto source).
 */
public class AlarmStateProjector {

    /** Resolves a service name (e.g. "ICMP") to its {@code serviceid} PK; returns {@code null} on unknown. */
    private final Function<String, Integer> serviceTypeResolver;

    public AlarmStateProjector(Function<String, Integer> serviceTypeResolver) {
        this.serviceTypeResolver = serviceTypeResolver;
    }

    public Object[] project(AlarmState a) {
        Timestamp lastEventTs = a.getLastEventTimeMs() > 0 ? new Timestamp(a.getLastEventTimeMs()) : null;
        Long nodeIdLong = a.getNodeId() > 0 ? (long) a.getNodeId() : null;
        return new Object[] {
                a.getUei(),                                        // 0 eventuei
                a.getNodeId() > 0 ? a.getNodeId() : null,          // 1 nodeid
                emptyToNull(a.getIpAddress()),                     // 2 ipaddr
                a.getServiceName().isEmpty() ? null : serviceTypeResolver.apply(a.getServiceName()),  // 3 serviceid
                a.getReductionKey(),                               // 4 reductionkey
                1,                                                 // 5 alarmtype — fixed: problem alarm
                a.getCount(),                                      // 6 counter
                a.getSeverity().getNumber(),                       // 7 severity
                a.getFirstEventTimeMs() > 0 ? new Timestamp(a.getFirstEventTimeMs()) : null,
                lastEventTs,                                       // 9 lasteventtime
                a.getAckTimeMs() > 0 ? new Timestamp(a.getAckTimeMs()) : null,
                emptyToNull(a.getAckUser()),                       // 11 alarmackuser
                a.getDescription(),                                // 12 description
                a.getLogMessage(),                                 // 13 logmsg
                a.getIfIndex() > 0 ? a.getIfIndex() : null,        // 14 ifindex
                a.getLocation(),                                   // 15 location
                a.getUei(),                                        // 16 event_uei
                a.getSeverity().getNumber(),                       // 17 event_severity
                lastEventTs,                                       // 18 event_timestamp
                nodeIdLong,                                        // 19 event_node_id (Long type for the BIGINT column)
                a.getLogMessage()                                  // 20 event_log_msg
        };
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }
}
