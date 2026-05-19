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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code deltav.alarmd.kafka-publisher.*}. Daemon-scoped per the
 * project's no-shared-config rule.
 */
@ConfigurationProperties(prefix = "deltav.alarmd.kafka-publisher")
public class AlarmPublisherProperties {

    /** Whether the publisher is active. Default true. */
    private boolean enabled = true;

    /** Kafka bootstrap servers. */
    private String bootstrapServers = "kafka:9092";

    /** Target topic. */
    private String topic = "deltav-alarms-state-change";

    /** Number of partitions for the alarms topic. Default 8. */
    private int partitions = 8;

    /** Replication factor for the alarms topic. Default 1. */
    private short replicationFactor = 1;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int v) { this.partitions = v; }

    public short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(short v) { this.replicationFactor = v; }
}
