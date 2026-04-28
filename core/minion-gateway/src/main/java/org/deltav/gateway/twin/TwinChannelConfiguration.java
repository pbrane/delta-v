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
package org.deltav.gateway.twin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring Kafka container factory for the Twin response listener.
 * {@code @EnableKafka} is on
 * {@link org.deltav.gateway.rpc.RpcChannelConfiguration} (PR1); the annotation
 * is process-wide, so duplicating it here would be redundant. If that
 * annotation is ever removed, the {@link TwinChannelDispatcher}'s
 * {@code @KafkaListener} will silently never receive messages — the same
 * silent-failure mode that surfaced at PR1 Phase 4.
 */
@Configuration
public class TwinChannelConfiguration {

    @Bean
    public ConsumerFactory<String, byte[]> twinResponseConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // Twin: read from earliest at startup so we rebuild state from history
        // (Decision 2 sub-decision 2-iii). After replay, group offsets persist
        // across gateway restarts, so a fresh start re-reads only what's new.
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new DefaultKafkaConsumerFactory<>(cfg);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> twinResponseContainerFactory(
            ConsumerFactory<String, byte[]> twinResponseConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> f =
            new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(twinResponseConsumerFactory);
        return f;
    }
}
