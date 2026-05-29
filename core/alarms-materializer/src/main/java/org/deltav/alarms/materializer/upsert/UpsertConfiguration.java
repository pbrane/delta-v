/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.upsert;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UpsertConfiguration {

    @Bean
    public AlarmStateProjector alarmStateProjector(ServiceTypeResolver resolver) {
        return new AlarmStateProjector(resolver::resolve);
    }

    /** Used by Task 10's PredicateEvaluator; lives here as the shared "infrastructure" bean. */
    @Bean
    public Clock systemClock() {
        return Clock.systemUTC();
    }
}
