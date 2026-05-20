/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder;

import org.deltav.alerts.forwarder.config.AlertsForwarderProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point for the Delta-V alerts-forwarder daemon. Consumes
 * {@code deltav-alarms-state-change}, enriches against a local materialization
 * of {@code deltav-node-context}, and forwards firing/resolved alerts through
 * the {@link org.deltav.alerts.forwarder.sink.AlarmSink} seam.
 *
 * <p>v1.3.0 "Alarms on Kafka" Track 2b. See
 * {@code docs/superpowers/specs/2026-05-19-v1.3.0-track2-alerts-forwarder-design.md}.
 */
@SpringBootApplication(scanBasePackages = "org.deltav.alerts.forwarder")
@EnableConfigurationProperties(AlertsForwarderProperties.class)
public class AlertsForwarderApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlertsForwarderApplication.class, args);
    }
}
