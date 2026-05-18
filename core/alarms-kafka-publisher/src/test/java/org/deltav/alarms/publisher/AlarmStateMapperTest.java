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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;

import org.deltav.alarms.proto.AlarmState;
import org.junit.jupiter.api.Test;
import org.opennms.netmgt.model.OnmsAlarm;
import org.opennms.netmgt.model.OnmsNode;
import org.opennms.netmgt.model.OnmsSeverity;
import org.opennms.netmgt.model.monitoringLocations.OnmsMonitoringLocation;

public class AlarmStateMapperTest {

    @Test
    void mapsCoreFields() {
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setId(42);
        alarm.setReductionKey("uei.opennms.org/nodes/nodeDown::1");
        alarm.setUei("uei.opennms.org/nodes/nodeDown");
        alarm.setSeverity(OnmsSeverity.MAJOR);
        alarm.setCounter(3);
        alarm.setFirstEventTime(new Date(1_000L));
        alarm.setLastEventTime(new Date(2_000L));

        AlarmState proto = new AlarmStateMapper().toProto(alarm);

        assertThat(proto.getAlarmId()).isEqualTo(42);
        assertThat(proto.getReductionKey()).isEqualTo("uei.opennms.org/nodes/nodeDown::1");
        assertThat(proto.getUei()).isEqualTo("uei.opennms.org/nodes/nodeDown");
        assertThat(proto.getSeverity()).isEqualTo(AlarmState.Severity.MAJOR);
        assertThat(proto.getCount()).isEqualTo(3);
        assertThat(proto.getFirstEventTimeMs()).isEqualTo(1_000L);
        assertThat(proto.getLastEventTimeMs()).isEqualTo(2_000L);
    }

    @Test
    void nullNodeYieldsZeroIdAndDefaultLocation() {
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setId(1);
        alarm.setReductionKey("rk");
        alarm.setSeverity(OnmsSeverity.NORMAL);

        AlarmState proto = new AlarmStateMapper().toProto(alarm);

        assertThat(proto.getNodeId()).isEqualTo(0);
        assertThat(proto.getLocation()).isEqualTo("Default");
    }

    @Test
    void nullSeverityMapsToIndeterminate() {
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setId(1);
        alarm.setReductionKey("rk");

        AlarmState proto = new AlarmStateMapper().toProto(alarm);

        assertThat(proto.getSeverity()).isEqualTo(AlarmState.Severity.INDETERMINATE);
    }

    @Test
    void locationResolvedFromNodeMonitoringLocation() {
        OnmsMonitoringLocation location = new OnmsMonitoringLocation("Durham", "Durham");
        OnmsNode node = new OnmsNode();
        node.setLocation(location);

        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setId(1);
        alarm.setReductionKey("rk");
        alarm.setNode(node);

        AlarmState proto = new AlarmStateMapper().toProto(alarm);

        assertThat(proto.getLocation()).isEqualTo("Durham");
    }

    @Test
    void acknowledgementFieldsAreMapped() {
        OnmsAlarm alarm = new OnmsAlarm();
        alarm.setId(1);
        alarm.setReductionKey("rk");
        alarm.setAlarmAckUser("admin");
        alarm.setAlarmAckTime(new Date(5000L));

        AlarmState proto = new AlarmStateMapper().toProto(alarm);

        assertThat(proto.getAckUser()).isEqualTo("admin");
        assertThat(proto.getAckTimeMs()).isEqualTo(5000L);
    }
}
