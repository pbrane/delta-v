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
package org.deltav.alarms.publisher;

import java.util.Date;
import java.util.function.Consumer;

import org.deltav.alarms.proto.AlarmState;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsSeverity;

/**
 * Pure function: converts an {@link OnmsAlarm} entity into the thin
 * {@link AlarmState} wire record. No I/O, no Spring — trivially unit-testable.
 */
public class AlarmStateMapper {

    private static final String DEFAULT_LOCATION = "Default";

    /**
     * Converts an {@link OnmsAlarm} to its {@link AlarmState} wire representation.
     * Always returns a non-null instance: missing numeric fields become {@code 0},
     * missing string fields become empty string, a missing severity becomes
     * {@code INDETERMINATE}, and a missing node location becomes {@code "Default"}.
     * The caller must supply a non-null alarm; the publisher filters nulls upstream.
     */
    public AlarmState toProto(OnmsAlarm alarm) {
        AlarmState.Builder b = AlarmState.newBuilder()
                .setAlarmId(alarm.getId() != null ? alarm.getId() : 0)
                .setSeverity(mapSeverity(alarm.getSeverity()))
                .setCount(alarm.getCounter() != null ? alarm.getCounter() : 0)
                .setFirstEventTimeMs(epochMillis(alarm.getFirstEventTime()))
                .setLastEventTimeMs(epochMillis(alarm.getLastEventTime()))
                .setAckTimeMs(epochMillis(alarm.getAlarmAckTime()));
        setIfNotNull(alarm.getReductionKey(), b::setReductionKey);
        setIfNotNull(alarm.getUei(), b::setUei);
        setIfNotNull(alarm.getAlarmAckUser(), b::setAckUser);
        setIfNotNull(alarm.getDescription(), b::setDescription);
        setIfNotNull(alarm.getLogMsg(), b::setLogMessage);
        b.setNodeId(alarm.getNodeId() != null ? alarm.getNodeId() : 0);
        b.setLocation(resolveLocation(alarm));
        return b.build();
    }

    private static String resolveLocation(OnmsAlarm alarm) {
        if (alarm.getNode() != null && alarm.getNode().getLocation() != null
                && alarm.getNode().getLocation().getLocationName() != null) {
            return alarm.getNode().getLocation().getLocationName();
        }
        return DEFAULT_LOCATION;
    }

    private static AlarmState.Severity mapSeverity(OnmsSeverity severity) {
        if (severity == null) {
            return AlarmState.Severity.INDETERMINATE;
        }
        switch (severity) {
            case CLEARED:  return AlarmState.Severity.CLEARED;
            case NORMAL:   return AlarmState.Severity.NORMAL;
            case WARNING:  return AlarmState.Severity.WARNING;
            case MINOR:    return AlarmState.Severity.MINOR;
            case MAJOR:    return AlarmState.Severity.MAJOR;
            case CRITICAL: return AlarmState.Severity.CRITICAL;
            case INDETERMINATE:
            default:       return AlarmState.Severity.INDETERMINATE;
        }
    }

    private static long epochMillis(Date date) {
        return date != null ? date.getTime() : 0L;
    }

    private static void setIfNotNull(String value, Consumer<String> setter) {
        if (value != null) {
            setter.accept(value);
        }
    }
}
