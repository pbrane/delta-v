/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.filter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.deltav.alarms.proto.AlarmState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides whether an alarm is forwarded. An alarm passes when its severity is
 * at or above the configured minimum AND its UEI is not denylisted. A cleared
 * alarm (severity {@code CLEARED}, enum number 1) is always below a valid
 * minimum, so it fails the filter — the pipeline maps a filter failure on a
 * currently-active alarm to a resolve.
 */
public class AlarmFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmFilter.class);

    private final int minSeverityNumber;
    private final Set<String> ueiDenylist;

    public AlarmFilter(String minSeverity, List<String> ueiDenylist) {
        this.minSeverityNumber = parseMinSeverity(minSeverity);
        this.ueiDenylist = new HashSet<>(ueiDenylist);
    }

    public boolean accept(AlarmState alarm) {
        if (alarm.getSeverity().getNumber() < minSeverityNumber) {
            return false;
        }
        return !ueiDenylist.contains(alarm.getUei());
    }

    private static int parseMinSeverity(String configured) {
        try {
            return AlarmState.Severity.valueOf(configured.trim().toUpperCase()).getNumber();
        } catch (RuntimeException e) {
            LOG.warn("Invalid deltav.alerts-forwarder.filter.min-severity '{}' — defaulting to WARNING",
                    configured);
            return AlarmState.Severity.WARNING.getNumber();
        }
    }
}
