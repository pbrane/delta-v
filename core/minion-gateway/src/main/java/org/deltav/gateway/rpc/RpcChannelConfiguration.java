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
package org.deltav.gateway.rpc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class RpcChannelConfiguration {

    @Bean
    public KafkaProducer<String, byte[]> minionGatewayKafkaProducer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        return new KafkaProducer<>(p);
    }

    @Bean
    public ConsumerFactory<String, byte[]> rpcRequestConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new DefaultKafkaConsumerFactory<>(cfg);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> rpcRequestContainerFactory(
            ConsumerFactory<String, byte[]> rpcRequestConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> f =
            new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(rpcRequestConsumerFactory);
        return f;
    }
}
