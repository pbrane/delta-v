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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opennms.netmgt.dao.api.AlarmEntityListener;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsSeverity;

class CountingAlarmEntityNotifierTest {

    @Test
    void forwardsCreateToRegisteredListeners() {
        AlarmEntityListener listener = Mockito.mock(AlarmEntityListener.class);
        CountingAlarmEntityNotifier notifier =
                new CountingAlarmEntityNotifier(new SimpleMeterRegistry(), List.of(listener));
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setSeverity(OnmsSeverity.MAJOR);

        notifier.didCreateAlarm(alarm);

        verify(listener).onAlarmCreated(alarm);
        verifyNoMoreInteractions(listener);
    }

    @Test
    void forwardsDeleteToRegisteredListeners() {
        AlarmEntityListener listener = Mockito.mock(AlarmEntityListener.class);
        CountingAlarmEntityNotifier notifier =
                new CountingAlarmEntityNotifier(new SimpleMeterRegistry(), List.of(listener));
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setSeverity(OnmsSeverity.NORMAL);

        notifier.didDeleteAlarm(alarm);

        verify(listener).onAlarmDeleted(alarm);
    }

    @Test
    void forwardsSeverityUpdateToRegisteredListeners() {
        AlarmEntityListener listener = Mockito.mock(AlarmEntityListener.class);
        CountingAlarmEntityNotifier notifier =
                new CountingAlarmEntityNotifier(new SimpleMeterRegistry(), List.of(listener));
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setSeverity(OnmsSeverity.CRITICAL);

        notifier.didUpdateAlarmSeverity(alarm, OnmsSeverity.MINOR);

        verify(listener).onAlarmSeverityUpdated(alarm, OnmsSeverity.MINOR);
    }

    @Test
    void emptyListenerListIsHarmless() {
        CountingAlarmEntityNotifier notifier =
                new CountingAlarmEntityNotifier(new SimpleMeterRegistry(), List.of());
        notifier.didCreateAlarm(new OnmsAlarm()); // must not throw
    }

    @Test
    void oneFailingListenerDoesNotBlockOthers() {
        AlarmEntityListener broken = Mockito.mock(AlarmEntityListener.class);
        Mockito.doThrow(new RuntimeException("broken listener"))
               .when(broken).onAlarmCreated(org.mockito.ArgumentMatchers.any());
        AlarmEntityListener healthy = Mockito.mock(AlarmEntityListener.class);

        CountingAlarmEntityNotifier notifier = new CountingAlarmEntityNotifier(
                new SimpleMeterRegistry(), List.of(broken, healthy));
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setSeverity(OnmsSeverity.MAJOR);

        notifier.didCreateAlarm(alarm); // must not throw despite the broken listener

        verify(healthy).onAlarmCreated(alarm); // healthy listener still invoked
    }
}
