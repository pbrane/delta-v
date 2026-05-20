/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.config;

import org.deltav.alerts.forwarder.filter.AlarmFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean factory for collaborators that need constructor args sourced from
 * {@link AlertsForwarderProperties} — Spring component-scan can't autowire
 * {@code String} / {@code List<String>} into a {@code @Component}, so we
 * hand-build them here.
 *
 * <p>Suffix is {@code Configuration} so the class's default bean name doesn't
 * collide with the {@code @Bean} method inside (per project convention —
 * {@code feedback_configuration_class_bean_name_collision}).</p>
 */
@Configuration
public class AlertsForwarderBeansConfiguration {

    @Bean
    public AlarmFilter alarmFilter(AlertsForwarderProperties props) {
        return new AlarmFilter(props.getFilter().getMinSeverity(),
                props.getFilter().getUeiDenylist());
    }
}
