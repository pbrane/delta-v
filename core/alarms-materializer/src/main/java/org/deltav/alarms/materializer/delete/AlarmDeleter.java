/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.delete;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Handles tombstone records by deleting the matching PG row. Does NOT produce
 * tombstones — the retention engine is the only tombstone producer
 * (spec §4.4). Idempotent: zero rows affected is fine (the row was already
 * removed or never existed).
 */
@Component
public class AlarmDeleter {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmDeleter.class);

    /** Package-visible for tests. */
    static final String SQL = "DELETE FROM alarms WHERE reductionkey = ?";

    private final JdbcTemplate jdbc;

    public AlarmDeleter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void delete(String reductionKey) {
        int affected = jdbc.update(SQL, reductionKey);
        if (affected == 0) {
            LOG.debug("Tombstone for rk={} matched no row (already removed or never existed)", reductionKey);
        }
    }
}
