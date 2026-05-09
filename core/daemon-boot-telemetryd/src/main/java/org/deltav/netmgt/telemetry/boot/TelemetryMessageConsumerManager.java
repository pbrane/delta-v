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
package org.deltav.netmgt.telemetry.boot;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.core.ipc.sink.api.Message;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.ipc.sink.common.AbstractMessageConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

/**
 * Multi-module MessageConsumerManager for Telemetryd.
 *
 * <p>Unlike the single-module LocalMessageConsumerManager (used by Trapd/Syslogd),
 * this manager spawns a separate KafkaSinkBridge per registered SinkModule.
 * Each bridge consumes from its own Kafka topic (e.g., DeltaV.Sink.Telemetry-Netflow-5).</p>
 *
 * <p>When {@code Telemetryd.start()} runs, it creates {@code TelemetrySinkModule} instances
 * per queue and registers consumers with this manager. The {@code startConsumingForModule()}
 * callback fires once per registered module, spawning a {@code KafkaSinkBridge} thread
 * for each.</p>
 */
public class TelemetryMessageConsumerManager extends AbstractMessageConsumerManager
        implements DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryMessageConsumerManager.class);

    private final Map<String, KafkaSinkBridge> bridges = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;

    public TelemetryMessageConsumerManager(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void startConsumingForModule(SinkModule<?, Message> module) {
        String moduleId = module.getId();
        LOG.info("Telemetry sink consumer registered for module: {}", moduleId);

        if (bridges.containsKey(moduleId)) {
            LOG.warn("Bridge already exists for module: {}", moduleId);
            return;
        }

        KafkaSinkBridge bridge = new KafkaSinkBridge(this, meterRegistry);
        bridge.setModule(module);
        bridges.put(moduleId, bridge);

        try {
            bridge.afterPropertiesSet();
            LOG.info("KafkaSinkBridge started for telemetry module: {}", moduleId);
        } catch (Exception e) {
            LOG.error("Failed to start KafkaSinkBridge for module {}: {}", moduleId, e.getMessage(), e);
            bridges.remove(moduleId);
        }
    }

    @Override
    protected void stopConsumingForModule(SinkModule<?, Message> module) {
        String moduleId = module.getId();
        KafkaSinkBridge bridge = bridges.remove(moduleId);
        if (bridge != null) {
            bridge.destroy();
            LOG.info("KafkaSinkBridge stopped for telemetry module: {}", moduleId);
        }
    }

    @Override
    public void destroy() {
        for (Map.Entry<String, KafkaSinkBridge> entry : bridges.entrySet()) {
            entry.getValue().destroy();
            LOG.info("KafkaSinkBridge destroyed for module: {}", entry.getKey());
        }
        bridges.clear();
    }
}
