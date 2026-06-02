/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.startup;

import org.deltav.prometheus.writer.config.PrometheusWriterProperties;
import org.deltav.nodecontext.NodeContextCacheReadyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binding.BindingsLifecycleController;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Resumes the paused {@code timeseriesConsumer-in-0} binding once the NodeContext
 * cache has finished its initial bootstrap. Counterpart to
 * {@link TimeseriesBindingStartupGate}.
 *
 * <p>No-op when {@code prometheus-writer.startup-gate.enabled=false}.</p>
 */
@Component
public class TimeseriesBindingResumer {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesBindingResumer.class);
    private static final String BINDING = "timeseriesConsumer-in-0";

    private final BindingsLifecycleController lifecycle;
    private final PrometheusWriterProperties props;

    public TimeseriesBindingResumer(BindingsLifecycleController lifecycle,
                                    PrometheusWriterProperties props) {
        this.lifecycle = lifecycle;
        this.props = props;
    }

    @EventListener
    public void onReady(NodeContextCacheReadyEvent event) {
        if (!props.startupGate().enabled()) {
            return;
        }
        LOG.info("NodeContextCache ready (size={}, bootstrap={}ms) — resuming binding {}",
                event.getCacheSize(), event.getBootstrapDurationMs(), BINDING);
        lifecycle.resume(BINDING);
    }
}
