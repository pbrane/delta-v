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
package org.deltav.horizon.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Mirrors every metric in a Dropwizard {@link MetricRegistry} into Spring
 * Boot's Micrometer {@link MeterRegistry} so that metrics registered via the
 * Dropwizard API become scrapable at {@code /actuator/prometheus}.
 *
 * <p>Horizon code (alarm processing, RPC clients, provisioning, collection,
 * polling, telemetry parsers, etc.) uses Dropwizard's {@link MetricRegistry}
 * API directly. Micrometer's built-in {@code DropwizardMeterRegistry} does
 * <em>not</em> pick those up — it only tracks meters created through the
 * Micrometer API, using Dropwizard as backing storage. Dropwizard's
 * {@link MetricRegistry#addListener(MetricRegistryListener)} documents that
 * "the listener will be notified of all existing metrics when it first
 * registers," so we attach a listener at {@link #bindTo(MeterRegistry)} time
 * and create Micrometer mirrors (read-through wrappers) for every existing
 * and future Dropwizard metric.
 *
 * <p>This is implemented as a {@link MeterBinder} so Spring Boot's
 * {@code MeterRegistryPostProcessor} calls {@link #bindTo(MeterRegistry)}
 * with the composite {@code MeterRegistry} after all beans (including those
 * that populate the Dropwizard registry) have been constructed. The
 * fire-on-attach behavior of {@link MetricRegistry#addListener} then
 * surfaces every pre-existing metric immediately. Late-arriving meters
 * (horizon registers many lazily) are surfaced via the same listener.
 *
 * <p>Naming: each Dropwizard metric name gets the configured prefix
 * prepended (e.g. {@code "opennms"} → {@code
 * opennms.org.opennms.netmgt.collectd.persist}). Micrometer's Prometheus
 * naming convention flattens dots to underscores and applies snake_case,
 * producing metric names such as
 * {@code opennms_org_opennms_netmgt_collectd_persist} at the Prometheus
 * endpoint.
 *
 * <p>Histogram percentile data is intentionally not exposed — only the
 * count is mirrored as a {@link FunctionCounter}. If a future caller
 * registers histograms whose distribution is operationally meaningful,
 * extend {@link MirroringListener#onHistogramAdded} to build a Micrometer
 * {@code DistributionSummary} via {@code Histogram.getSnapshot()}.
 *
 * <p>Bind-once contract: a single bridge instance must not be bound to
 * multiple {@link MeterRegistry}s. Each Dropwizard registry that needs
 * surfacing should own one bridge bean.
 */
public final class HorizonMetricsBridge implements MeterBinder {

    private final MetricRegistry dropwizardRegistry;
    private final String prefix;

    /**
     * @param dropwizardRegistry the Dropwizard registry whose meters should be
     *     mirrored — must be the same instance horizon code receives
     * @param prefix dot-separated prefix applied to every mirrored meter name;
     *     becomes the leading underscore-segment in Prometheus output
     */
    public HorizonMetricsBridge(MetricRegistry dropwizardRegistry, String prefix) {
        this.dropwizardRegistry = dropwizardRegistry;
        this.prefix = prefix;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        dropwizardRegistry.addListener(new MirroringListener(registry, prefix));
    }

    private static final class MirroringListener implements MetricRegistryListener {

        private final MeterRegistry registry;
        private final String prefix;

        MirroringListener(MeterRegistry registry, String prefix) {
            this.registry = registry;
            this.prefix = prefix;
        }

        @Override
        public void onGaugeAdded(String name, com.codahale.metrics.Gauge<?> gauge) {
            Gauge.builder(prefixed(name), gauge, MirroringListener::gaugeValue)
                    .register(registry);
        }

        @Override
        public void onCounterAdded(String name, Counter counter) {
            FunctionCounter.builder(prefixed(name), counter, Counter::getCount)
                    .register(registry);
        }

        @Override
        public void onHistogramAdded(String name, Histogram histogram) {
            FunctionCounter.builder(prefixed(name) + ".count", histogram, Histogram::getCount)
                    .register(registry);
        }

        @Override
        public void onMeterAdded(String name, com.codahale.metrics.Meter meter) {
            FunctionCounter.builder(prefixed(name), meter, com.codahale.metrics.Meter::getCount)
                    .register(registry);
        }

        @Override
        public void onTimerAdded(String name, Timer timer) {
            FunctionTimer.builder(prefixed(name), timer,
                            Timer::getCount,
                            t -> t.getSnapshot().getMean(),
                            TimeUnit.NANOSECONDS)
                    .register(registry);
        }

        @Override public void onGaugeRemoved(String name) {}
        @Override public void onCounterRemoved(String name) {}
        @Override public void onHistogramRemoved(String name) {}
        @Override public void onMeterRemoved(String name) {}
        @Override public void onTimerRemoved(String name) {}

        private String prefixed(String dropwizardName) {
            return prefix + "." + dropwizardName;
        }

        private static double gaugeValue(com.codahale.metrics.Gauge<?> gauge) {
            Object value = gauge.getValue();
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            return Double.NaN;
        }
    }
}
