/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.startup;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Pauses the {@code timeseriesConsumer-in-0} Kafka listener container at factory time
 * so that the binding does not start consuming until {@link
 * org.deltav.nodecontext.NodeContextCacheReadyEvent} fires.
 *
 * <p>Gated by {@code prometheus-writer.startup-gate.enabled} (default {@code true}).
 * When disabled the container starts unpaused — useful for unit tests that don't run
 * the bootstrap consumer.</p>
 *
 * <p>Note: in Spring Cloud Stream 5.0.x the customizer interface lives in
 * {@code org.springframework.cloud.stream.config}, not in the kafka-binder
 * {@code config} sub-package referenced by older (4.x) docs.</p>
 */
@Component
public class TimeseriesBindingStartupGate
        implements ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesBindingStartupGate.class);
    private static final String BINDING = "timeseriesConsumer-in-0";

    private final PrometheusWriterProperties props;

    public TimeseriesBindingStartupGate(PrometheusWriterProperties props) {
        this.props = props;
    }

    @Override
    public void configure(AbstractMessageListenerContainer<?, ?> container,
                          String destinationName,
                          String group) {
        if (!props.startupGate().enabled()) {
            LOG.info("Startup gate disabled; timeseries binding will start unpaused");
            return;
        }
        LOG.info("Pausing timeseries binding '{}' — will resume on NodeContextCacheReadyEvent", BINDING);
        container.pause();
    }
}
