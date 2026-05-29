/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer;

import org.deltav.alarms.materializer.config.MaterializerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Delta-V alarms-materializer daemon. Consumes
 * {@code deltav-alarms-state-change} and projects it into the PostgreSQL
 * {@code alarms} table; owns retention/GC via declarative predicate rules.
 *
 * <p>v1.3.0 "Alarms on Kafka" Track 3. See
 * {@code docs/superpowers/specs/2026-05-22-v1.3.0-track3-alarm-materializer-design.md}.
 */
@SpringBootApplication(scanBasePackages = "org.deltav.alarms.materializer")
@EnableConfigurationProperties(MaterializerProperties.class)
@EnableScheduling
public class AlarmsMaterializerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlarmsMaterializerApplication.class, args);
    }
}
