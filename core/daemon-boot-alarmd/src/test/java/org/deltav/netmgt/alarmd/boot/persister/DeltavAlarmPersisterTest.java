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
package org.deltav.netmgt.alarmd.boot.persister;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.deltav.alarms.proto.AlarmState;
import org.deltav.netmgt.alarmd.boot.cache.ReductionCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.alarmd.AlarmPersisterImpl;
import org.opennms.netmgt.dao.api.AlarmEntityNotifier;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsSeverity;
import org.opennms.netmgt.xml.event.AlarmData;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Logmsg;

class DeltavAlarmPersisterTest {

    private static final String REDUCTION_KEY = "uei.opennms.org/test:foo:1";

    private AlarmPersisterImpl dualWriteDelegate;
    private ReductionCache reductionCache;
    private AlarmEntityNotifier notifier;
    private AlarmdPersistenceProperties properties;

    @BeforeEach
    void setUp() {
        dualWriteDelegate = mock(AlarmPersisterImpl.class);
        reductionCache = new ReductionCache();
        notifier = mock(AlarmEntityNotifier.class);
        properties = new AlarmdPersistenceProperties();
    }

    @Test
    void kafkaOnlyModeUsesCacheNotDao() {
        properties.setMode("kafka-only");
        DeltavAlarmPersister persister = new DeltavAlarmPersister(
                dualWriteDelegate, reductionCache, notifier, properties);

        // cache empty → fresh alarm
        Event event = newEventWithReductionKey(REDUCTION_KEY, "Major", "first occurrence");

        OnmsAlarm result = persister.persist(event);

        assertThat(result).isNotNull();
        assertThat(result.getReductionKey()).isEqualTo(REDUCTION_KEY);
        assertThat(result.getCounter()).isEqualTo(1);
        assertThat(result.getSeverity()).isEqualTo(OnmsSeverity.MAJOR);
        assertThat(result.getUei()).isEqualTo("uei.opennms.org/test");
        verify(notifier).didCreateAlarm(result);
        verify(notifier, never()).didUpdateAlarmWithReducedEvent(any());
        verify(dualWriteDelegate, never()).persist(any());
    }

    @Test
    void kafkaOnlyModeIncrementsCounterOnSubsequentEvent() {
        properties.setMode("kafka-only");
        // pre-populate the cache with an existing alarm at count=4
        reductionCache.applyUpdate(REDUCTION_KEY, AlarmState.newBuilder()
                .setAlarmId(42)
                .setReductionKey(REDUCTION_KEY)
                .setUei("uei.opennms.org/test")
                .setSeverity(AlarmState.Severity.WARNING)
                .setCount(4)
                .setFirstEventTimeMs(1_000_000L)
                .setLogMessage("cached log")
                .setDescription("cached desc")
                .build());

        DeltavAlarmPersister persister = new DeltavAlarmPersister(
                dualWriteDelegate, reductionCache, notifier, properties);

        Event event = newEventWithReductionKey(REDUCTION_KEY, "Critical", "5th occurrence");

        OnmsAlarm result = persister.persist(event);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(42);
        assertThat(result.getReductionKey()).isEqualTo(REDUCTION_KEY);
        assertThat(result.getCounter()).isEqualTo(5);
        assertThat(result.getSeverity()).isEqualTo(OnmsSeverity.CRITICAL);
        // firstEventTime preserved from cached state
        assertThat(result.getFirstEventTime().getTime()).isEqualTo(1_000_000L);
        // event log overrides cached log
        assertThat(result.getLogMsg()).isEqualTo("5th occurrence");
        verify(notifier).didUpdateAlarmWithReducedEvent(result);
        verify(notifier, never()).didCreateAlarm(any());
        verify(dualWriteDelegate, never()).persist(any());
    }

    @Test
    void dualWriteModeDelegatesToWrappedImpl() {
        properties.setMode("dual-write");
        OnmsAlarm delegateReturn = new OnmsAlarm();
        delegateReturn.setReductionKey(REDUCTION_KEY);
        delegateReturn.setSeverity(OnmsSeverity.MAJOR);
        when(dualWriteDelegate.persist(any())).thenReturn(delegateReturn);

        DeltavAlarmPersister persister = new DeltavAlarmPersister(
                dualWriteDelegate, reductionCache, notifier, properties);

        Event event = newEventWithReductionKey(REDUCTION_KEY, "Major", "delegated");

        OnmsAlarm result = persister.persist(event);

        verify(dualWriteDelegate).persist(event);
        assertThat(result).isSameAs(delegateReturn);
        // kafka-only path's notifier calls MUST NOT fire — delegate owns notification
        verify(notifier, never()).didCreateAlarm(any());
        verify(notifier, never()).didUpdateAlarmWithReducedEvent(any());
    }

    // ---- helpers ----------------------------------------------------------

    private static Event newEventWithReductionKey(String rk, String severityLabel, String logContent) {
        Event event = new Event();
        event.setUei("uei.opennms.org/test");
        event.setSeverity(severityLabel);
        event.setDescr("descr");
        Logmsg logmsg = new Logmsg();
        logmsg.setContent(logContent);
        event.setLogmsg(logmsg);
        AlarmData alarmData = new AlarmData();
        alarmData.setReductionKey(rk);
        event.setAlarmData(alarmData);
        return event;
    }
}
