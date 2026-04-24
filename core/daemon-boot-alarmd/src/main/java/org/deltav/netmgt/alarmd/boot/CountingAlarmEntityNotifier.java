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
package org.deltav.netmgt.alarmd.boot;

import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.ALARMS_ACKNOWLEDGED;
import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.ALARMS_ARCHIVED;
import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.ALARMS_CREATED;
import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.ALARMS_DELETED;
import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.ALARMS_REDUCED;
import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.ALARMS_SEVERITY_UPDATED;
import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.ALARMS_UNACKNOWLEDGED;
import static org.deltav.netmgt.alarmd.boot.AlarmdDomainMetrics.TAG_SEVERITY;

import java.util.Date;
import java.util.Set;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.netmgt.dao.api.AlarmEntityNotifier;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsMemo;
import org.opennms.netmgt.model.OnmsReductionKeyMemo;
import org.opennms.netmgt.model.OnmsSeverity;
import org.opennms.netmgt.model.TroubleTicketState;

/**
 * {@link AlarmEntityNotifier} that counts every lifecycle notification it
 * receives via Micrometer. Replaces the no-op anonymous implementation
 * previously declared inline in {@code AlarmdConfiguration}.
 *
 * <p>Downstream listener integration (BSMd, REST callers) is tracked
 * separately in memory {@code project_alarmd_alarm_lifecycle_gap}. This
 * class exists purely to surface domain signal — it does not forward
 * events anywhere.
 */
public class CountingAlarmEntityNotifier implements AlarmEntityNotifier {

    private final MeterRegistry registry;

    public CountingAlarmEntityNotifier(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void didCreateAlarm(OnmsAlarm alarm) {
        registry.counter(ALARMS_CREATED, TAG_SEVERITY, severity(alarm)).increment();
    }

    @Override
    public void didUpdateAlarmWithReducedEvent(OnmsAlarm alarm) {
        registry.counter(ALARMS_REDUCED, TAG_SEVERITY, severity(alarm)).increment();
    }

    @Override
    public void didAcknowledgeAlarm(OnmsAlarm alarm, String user, Date when) {
        registry.counter(ALARMS_ACKNOWLEDGED).increment();
    }

    @Override
    public void didUnacknowledgeAlarm(OnmsAlarm alarm, String user, Date when) {
        registry.counter(ALARMS_UNACKNOWLEDGED).increment();
    }

    @Override
    public void didUpdateAlarmSeverity(OnmsAlarm alarm, OnmsSeverity previous) {
        // Tag by *new* severity (what the alarm is now, which is the operationally
        // interesting dimension — "alarms that just became CRITICAL").
        registry.counter(ALARMS_SEVERITY_UPDATED, TAG_SEVERITY, severity(alarm)).increment();
    }

    @Override
    public void didArchiveAlarm(OnmsAlarm alarm, String previousReductionKey) {
        registry.counter(ALARMS_ARCHIVED).increment();
    }

    @Override
    public void didDeleteAlarm(OnmsAlarm alarm) {
        registry.counter(ALARMS_DELETED, TAG_SEVERITY, severity(alarm)).increment();
    }

    @Override public void didUpdateStickyMemo(OnmsAlarm a, String b, String au, Date d) {}
    @Override public void didUpdateReductionKeyMemo(OnmsAlarm a, String b, String au, Date d) {}
    @Override public void didDeleteStickyMemo(OnmsAlarm a, OnmsMemo m) {}
    @Override public void didDeleteReductionKeyMemo(OnmsAlarm a, OnmsReductionKeyMemo m) {}
    @Override public void didUpdateLastAutomationTime(OnmsAlarm a, Date d) {}
    @Override public void didUpdateRelatedAlarms(OnmsAlarm a, Set<OnmsAlarm> s) {}
    @Override public void didChangeTicketStateForAlarm(OnmsAlarm a, TroubleTicketState s) {}

    private static String severity(OnmsAlarm alarm) {
        OnmsSeverity s = alarm != null ? alarm.getSeverity() : null;
        return s != null ? s.getLabel() : OnmsSeverity.INDETERMINATE.getLabel();
    }
}
