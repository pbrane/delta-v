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
package org.deltav.minion.boot;

import io.micrometer.core.instrument.MeterRegistry;

import org.opennms.core.rpc.api.RpcModule;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-wraps every {@link RpcModule} bean produced by the Spring context
 * with {@link MeteredRpcModule}, so any future RPC module added to the
 * Minion picks up {@code deltav_minion_*} counters and timing without
 * needing per-config edits.
 *
 * <p>Implemented as a {@link BeanPostProcessor} rather than touching each
 * {@code @Bean} factory because the set of RPC modules is open-ended —
 * Echo, Detect, Collect, Poll, Ping, PingSweep, SnmpProxy today; future
 * modules added by importing additional starters tomorrow.
 *
 * <p>Uses {@link ObjectProvider} for {@link MeterRegistry} so this class
 * loads cleanly at very early phase of bean creation; the {@link MeterRegistry}
 * is resolved lazily on first wrap call (when actuator beans are ready).
 */
@Configuration
public class RpcModuleMetricsPostProcessor implements BeanPostProcessor {

    private final ObjectProvider<MeterRegistry> meterRegistryProvider;

    public RpcModuleMetricsPostProcessor(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        this.meterRegistryProvider = meterRegistryProvider;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (!(bean instanceof RpcModule<?, ?> module)) {
            return bean;
        }
        if (bean instanceof MeteredRpcModule<?, ?>) {
            return bean;
        }
        MeterRegistry registry = meterRegistryProvider.getIfAvailable();
        if (registry == null) {
            // MeterRegistry not yet available — actuator may initialize later.
            // Skipping the wrap is preferable to throwing; the underlying module
            // still functions, just without deltav_minion_* metrics.
            return bean;
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        RpcModule wrapped = new MeteredRpcModule((RpcModule) module, registry);
        return wrapped;
    }
}
