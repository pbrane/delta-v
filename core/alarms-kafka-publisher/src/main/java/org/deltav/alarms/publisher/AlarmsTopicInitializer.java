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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures the {@code deltav-alarms-state-change} topic exists with
 * {@code cleanup.policy=compact}. Track 2b's resolve-on-tombstone logic depends
 * on compaction: the latest record per reduction key must survive forever, and
 * a null-valued tombstone must eventually garbage-collect a deleted alarm.
 *
 * <p>Fresh broker: {@code createTopics} creates the topic compacted. Existing
 * broker (the topic was auto-created with {@code cleanup.policy=delete}):
 * {@code createTopics} fails with {@link TopicExistsException} and we fall
 * through to {@code incrementalAlterConfigs}, switching the live topic to
 * compaction without data loss.</p>
 *
 * <p>Invoked once at startup by {@link AlarmPublisherConfiguration}. Failures
 * are logged, not fatal — a misconfigured topic degrades Track 2b behaviour
 * but must not stop alarmd from booting.</p>
 */
public final class AlarmsTopicInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmsTopicInitializer.class);

    private AlarmsTopicInitializer() {
    }

    /** Builds the desired compacted-topic spec. Package-visible for unit testing. */
    static NewTopic compactedTopic(String name, int partitions, short replicationFactor) {
        return new NewTopic(name, partitions, replicationFactor)
                .configs(Map.of(
                        "cleanup.policy", "compact",
                        "retention.ms", "-1",
                        "min.compaction.lag.ms", "60000",
                        "delete.retention.ms", "86400000"));
    }

    /**
     * Creates the topic compacted, or — if it already exists — alters its
     * config to compaction. Best-effort: any failure is logged and swallowed.
     */
    public static void ensureCompacted(String bootstrapServers, String topic,
                                       int partitions, short replicationFactor) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(props)) {
            try {
                admin.createTopics(List.of(compactedTopic(topic, partitions, replicationFactor)))
                        .all().get();
                LOG.info("Created compacted topic {}", topic);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    alterToCompact(admin, topic);
                } else {
                    LOG.warn("Could not create topic {} — Track 2b compaction not guaranteed", topic, e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted ensuring compacted topic {}", topic, e);
        } catch (Exception e) {
            LOG.warn("Could not ensure compacted topic {} — Track 2b compaction not guaranteed", topic, e);
        }
    }

    private static void alterToCompact(Admin admin, String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Map<ConfigResource, java.util.Collection<AlterConfigOp>> ops = Map.of(
                resource, List.of(
                        new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(new ConfigEntry("retention.ms", "-1"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(new ConfigEntry("min.compaction.lag.ms", "60000"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(new ConfigEntry("delete.retention.ms", "86400000"), AlterConfigOp.OpType.SET)));
        try {
            admin.incrementalAlterConfigs(ops).all().get();
            LOG.info("Altered existing topic {} to cleanup.policy=compact", topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted altering topic {} to compact", topic, e);
        } catch (Exception e) {
            LOG.warn("Could not alter topic {} to compact — Track 2b compaction not guaranteed", topic, e);
        }
    }
}
