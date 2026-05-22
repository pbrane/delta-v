/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.retention;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.deltav.alarms.materializer.config.MaterializerProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RetentionConfiguration {

    @Bean
    public List<RetentionRule> retentionRules(MaterializerProperties props, RetentionRulesValidator validator) {
        return validator.validate(props.getRetention().getRules());
    }

    @Bean(destroyMethod = "close")
    public Producer<String, byte[]> tombstoneProducer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "alarms-materializer-retention");
        return new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
    }
}
