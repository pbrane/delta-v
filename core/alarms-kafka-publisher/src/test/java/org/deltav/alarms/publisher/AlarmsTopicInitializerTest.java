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

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AlarmsTopicInitializerTest {

    @Test
    void buildsCompactedTopicSpec() {
        NewTopic topic = AlarmsTopicInitializer.compactedTopic("deltav-alarms-state-change", 8, (short) 1);

        assertThat(topic.name()).isEqualTo("deltav-alarms-state-change");
        assertThat(topic.numPartitions()).isEqualTo(8);
        assertThat(topic.replicationFactor()).isEqualTo((short) 1);
        Map<String, String> cfg = topic.configs();
        assertThat(cfg).containsEntry("cleanup.policy", "compact");
        assertThat(cfg).containsEntry("retention.ms", "-1");
        assertThat(cfg).containsEntry("min.compaction.lag.ms", "60000");
        assertThat(cfg).containsEntry("delete.retention.ms", "86400000");
    }
}
