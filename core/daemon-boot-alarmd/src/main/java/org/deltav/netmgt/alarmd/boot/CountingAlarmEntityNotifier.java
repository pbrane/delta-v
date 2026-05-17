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
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.netmgt.dao.api.AlarmEntityListener;
import org.opennms.netmgt.dao.api.AlarmEntityNotifier;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsMemo;
import org.opennms.netmgt.model.OnmsReductionKeyMemo;
import org.opennms.netmgt.model.OnmsSeverity;
import org.opennms.netmgt.model.TroubleTicketState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AlarmEntityNotifier} that both increments Micrometer counters on every
 * lifecycle notification it receives AND fans each callback out to all registered
 * {@link AlarmEntityListener} beans.
 *
 * <p>This restores the full chain:
 * {@code AlarmPersisterImpl → AlarmEntityNotifier → AlarmEntityListener(s)
 * → AlarmLifecycleListenerManager → AlarmLifecycleListener(s)},
 * enabling real-time delivery to downstream components such as the Kafka
 * alarm publisher. Without listener fan-out only the 2-minute snapshot timer
 * in {@code AlarmLifecycleListenerManager} would fire.</p>
 *
 * <p>Listener failures are isolated per-listener — a failing listener does not
 * prevent the remaining listeners from receiving the callback.</p>
 */
public class CountingAlarmEntityNotifier implements AlarmEntityNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(CountingAlarmEntityNotifier.class);

    private final MeterRegistry registry;
    private final List<AlarmEntityListener> listeners;

    public CountingAlarmEntityNotifier(MeterRegistry registry, List<AlarmEntityListener> listeners) {
        this.registry = registry;
        this.listeners = listeners;
    }

    @Override
    public void didCreateAlarm(OnmsAlarm alarm) {
        registry.counter(ALARMS_CREATED, TAG_SEVERITY, severity(alarm)).increment();
        forEach(l -> l.onAlarmCreated(alarm));
    }

    @Override
    public void didUpdateAlarmWithReducedEvent(OnmsAlarm alarm) {
        registry.counter(ALARMS_REDUCED, TAG_SEVERITY, severity(alarm)).increment();
        forEach(l -> l.onAlarmUpdatedWithReducedEvent(alarm));
    }

    @Override
    public void didAcknowledgeAlarm(OnmsAlarm alarm, String user, Date when) {
        registry.counter(ALARMS_ACKNOWLEDGED).increment();
        forEach(l -> l.onAlarmAcknowledged(alarm, user, when));
    }

    @Override
    public void didUnacknowledgeAlarm(OnmsAlarm alarm, String user, Date when) {
        registry.counter(ALARMS_UNACKNOWLEDGED).increment();
        forEach(l -> l.onAlarmUnacknowledged(alarm, user, when));
    }

    @Override
    public void didUpdateAlarmSeverity(OnmsAlarm alarm, OnmsSeverity previous) {
        // Tag by *new* severity (what the alarm is now, which is the operationally
        // interesting dimension — "alarms that just became CRITICAL").
        registry.counter(ALARMS_SEVERITY_UPDATED, TAG_SEVERITY, severity(alarm)).increment();
        forEach(l -> l.onAlarmSeverityUpdated(alarm, previous));
    }

    @Override
    public void didArchiveAlarm(OnmsAlarm alarm, String previousReductionKey) {
        registry.counter(ALARMS_ARCHIVED).increment();
        forEach(l -> l.onAlarmArchived(alarm, previousReductionKey));
    }

    @Override
    public void didDeleteAlarm(OnmsAlarm alarm) {
        registry.counter(ALARMS_DELETED, TAG_SEVERITY, severity(alarm)).increment();
        forEach(l -> l.onAlarmDeleted(alarm));
    }

    @Override
    public void didUpdateStickyMemo(OnmsAlarm alarm, String previousBody, String previousAuthor, Date previousUpdated) {
        forEach(l -> l.onStickyMemoUpdated(alarm, previousBody, previousAuthor, previousUpdated));
    }

    @Override
    public void didUpdateReductionKeyMemo(OnmsAlarm alarm, String previousBody, String previousAuthor, Date previousUpdated) {
        forEach(l -> l.onReductionKeyMemoUpdated(alarm, previousBody, previousAuthor, previousUpdated));
    }

    @Override
    public void didDeleteStickyMemo(OnmsAlarm alarm, OnmsMemo memo) {
        forEach(l -> l.onStickyMemoDeleted(alarm, memo));
    }

    @Override
    public void didDeleteReductionKeyMemo(OnmsAlarm alarm, OnmsReductionKeyMemo memo) {
        forEach(l -> l.onReductionKeyMemoDeleted(alarm, memo));
    }

    @Override
    public void didUpdateLastAutomationTime(OnmsAlarm alarm, Date previousLastAutomationTime) {
        forEach(l -> l.onLastAutomationTimeUpdated(alarm, previousLastAutomationTime));
    }

    @Override
    public void didUpdateRelatedAlarms(OnmsAlarm alarm, Set<OnmsAlarm> previousRelatedAlarms) {
        forEach(l -> l.onRelatedAlarmsUpdated(alarm, previousRelatedAlarms));
    }

    @Override
    public void didChangeTicketStateForAlarm(OnmsAlarm alarm, TroubleTicketState previousState) {
        forEach(l -> l.onTicketStateChanged(alarm, previousState));
    }

    private void forEach(Consumer<AlarmEntityListener> callback) {
        for (AlarmEntityListener listener : listeners) {
            try {
                callback.accept(listener);
            } catch (Exception e) {
                LOG.error("Error invoking alarm entity listener {}; skipping.", listener, e);
            }
        }
    }

    private static String severity(OnmsAlarm alarm) {
        OnmsSeverity s = alarm != null ? alarm.getSeverity() : null;
        return s != null ? s.getLabel() : OnmsSeverity.INDETERMINATE.getLabel();
    }
}
