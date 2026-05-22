/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.upsert;

import org.deltav.alarms.proto.AlarmState;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Upserts an {@link AlarmState} into the {@code alarms} table using PostgreSQL's
 * {@code ON CONFLICT (reductionkey) DO UPDATE}. {@code alarmid} is omitted from
 * the INSERT column list so the database sequence assigns it for new rows; on
 * conflict the existing {@code alarmid} is preserved.
 *
 * <p>Column order MUST stay in lock-step with {@link AlarmStateProjector}.
 */
@Component
public class AlarmUpsertWriter {

    /** Package-visible for tests. */
    static final String SQL = """
            INSERT INTO alarms (
              eventuei, nodeid, ipaddr, serviceid, reductionkey, alarmtype,
              counter, severity, firsteventtime, lasteventtime,
              alarmacktime, alarmackuser, description, logmsg,
              ifindex, location,
              event_uei, event_severity, event_timestamp, event_node_id, event_log_msg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (reductionkey) DO UPDATE SET
              severity        = EXCLUDED.severity,
              counter         = EXCLUDED.counter,
              lasteventtime   = EXCLUDED.lasteventtime,
              alarmacktime    = EXCLUDED.alarmacktime,
              alarmackuser    = EXCLUDED.alarmackuser,
              description     = EXCLUDED.description,
              logmsg          = EXCLUDED.logmsg,
              ifindex         = EXCLUDED.ifindex,
              ipaddr          = EXCLUDED.ipaddr,
              serviceid       = EXCLUDED.serviceid,
              event_uei       = EXCLUDED.event_uei,
              event_severity  = EXCLUDED.event_severity,
              event_timestamp = EXCLUDED.event_timestamp,
              event_node_id   = EXCLUDED.event_node_id,
              event_log_msg   = EXCLUDED.event_log_msg
            """;

    private final JdbcTemplate jdbc;
    private final AlarmStateProjector projector;

    public AlarmUpsertWriter(JdbcTemplate jdbc, AlarmStateProjector projector) {
        this.jdbc = jdbc;
        this.projector = projector;
    }

    public void upsert(AlarmState alarm) {
        jdbc.update(SQL, projector.project(alarm));
    }
}
