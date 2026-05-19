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

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opennms.netmgt.alarmd.AlarmLifecycleListenerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the {@link KafkaAlarmPublisher} into alarmd and registers it with the
 * existing {@link AlarmLifecycleListenerManager} so it receives lifecycle
 * callbacks. Component-scanned by {@code AlarmdApplication}.
 */
@Configuration
@EnableConfigurationProperties(AlarmPublisherProperties.class)
@ConditionalOnProperty(prefix = "deltav.alarmd.kafka-publisher", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class AlarmPublisherConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmPublisherConfiguration.class);

    @Bean(destroyMethod = "close")
    public Producer<String, byte[]> alarmKafkaProducer(AlarmPublisherProperties props) {
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        p.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        p.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "alarmd-publisher");
        return new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
    }

    /**
     * Runs once at startup: ensures deltav-alarms-state-change is compacted.
     * An ApplicationRunner (not @PostConstruct) so it runs after the context
     * is fully refreshed and Kafka connectivity is the only dependency.
     */
    @Bean
    public ApplicationRunner alarmsTopicInitializerRunner(AlarmPublisherProperties properties) {
        return args -> AlarmsTopicInitializer.ensureCompacted(
                properties.getBootstrapServers(), properties.getTopic(),
                properties.getPartitions(), properties.getReplicationFactor());
    }

    @Bean
    public KafkaAlarmPublisher kafkaAlarmPublisher(Producer<String, byte[]> alarmKafkaProducer,
                                                   AlarmPublisherProperties props,
                                                   AlarmLifecycleListenerManager manager) {
        KafkaAlarmPublisher publisher =
                new KafkaAlarmPublisher(alarmKafkaProducer, props.getTopic(), new AlarmStateMapper());
        manager.onListenerRegistered(publisher, Collections.emptyMap());
        LOG.info("Registered KafkaAlarmPublisher; publishing alarm lifecycle to topic {}", props.getTopic());
        return publisher;
    }
}
