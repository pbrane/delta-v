/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alarms.materializer.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds {@code deltav.alarms-materializer.*}. */
@ConfigurationProperties(prefix = "deltav.alarms-materializer")
public class MaterializerProperties {

    private String alarmsTopic = "deltav-alarms-state-change";
    private final Retention retention = new Retention();

    public String getAlarmsTopic() { return alarmsTopic; }
    public void setAlarmsTopic(String v) { this.alarmsTopic = v; }
    public Retention getRetention() { return retention; }

    public static class Retention {
        private Duration cadence = Duration.ofMinutes(1);
        private List<Map<String, Object>> rules = new ArrayList<>();

        public Duration getCadence() { return cadence; }
        public void setCadence(Duration v) { this.cadence = v; }
        public List<Map<String, Object>> getRules() { return rules; }
        public void setRules(List<Map<String, Object>> v) { this.rules = v; }
    }
}
