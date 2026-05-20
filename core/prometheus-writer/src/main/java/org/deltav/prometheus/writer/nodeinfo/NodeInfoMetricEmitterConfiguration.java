/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.nodeinfo;

import org.deltav.prometheus.writer.nodecontext.NodeContextCache;
import org.deltav.prometheus.writer.rw.BatchingRwWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Wires {@link NodeInfoMetricEmitter} to the live {@link BatchingRwWriter}
 * remote-write path and drives it on a fixed schedule. {@code @EnableScheduling}
 * is already on {@code PrometheusWriterApplication}.
 */
@Configuration
public class NodeInfoMetricEmitterConfiguration {

    @Bean
    public NodeInfoMetricEmitter nodeInfoMetricEmitter(NodeContextCache cache, BatchingRwWriter writer) {
        return new NodeInfoMetricEmitter(cache, writer::add);
    }

    /** Drives the emitter sweep. Default 60s; override with prometheus-writer.node-info.emit-interval-ms. */
    @Component
    static class NodeInfoEmitterSchedule {
        private final NodeInfoMetricEmitter emitter;

        NodeInfoEmitterSchedule(NodeInfoMetricEmitter emitter) {
            this.emitter = emitter;
        }

        @Scheduled(fixedDelayString = "${prometheus-writer.node-info.emit-interval-ms:60000}")
        void tick() {
            emitter.sweep();
        }
    }
}
