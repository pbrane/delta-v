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

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;

class HorizonMetricsBridgeTest {

    private static final String PREFIX = "opennms";

    @Test
    void mirrorsMetersThatExistedBeforeBind() {
        MetricRegistry dropwizard = new MetricRegistry();
        Counter sourceCounter = dropwizard.counter("alarmd.events.received");
        sourceCounter.inc(7);

        SimpleMeterRegistry micrometer = new SimpleMeterRegistry();
        new HorizonMetricsBridge(dropwizard, PREFIX).bindTo(micrometer);

        assertThat(micrometer.find("opennms.alarmd.events.received").functionCounter())
                .isNotNull()
                .extracting(fc -> fc.count())
                .isEqualTo(7.0);
    }

    @Test
    void mirrorsMetersAddedAfterBind() {
        MetricRegistry dropwizard = new MetricRegistry();
        SimpleMeterRegistry micrometer = new SimpleMeterRegistry();
        new HorizonMetricsBridge(dropwizard, PREFIX).bindTo(micrometer);

        Counter lateCounter = dropwizard.counter("provisiond.nodes.scanned");
        lateCounter.inc(42);

        assertThat(micrometer.find("opennms.provisiond.nodes.scanned").functionCounter())
                .isNotNull()
                .extracting(fc -> fc.count())
                .isEqualTo(42.0);
    }

    @Test
    void counterMirrorIsReadThrough() {
        MetricRegistry dropwizard = new MetricRegistry();
        Counter source = dropwizard.counter("collectd.persists");
        SimpleMeterRegistry micrometer = new SimpleMeterRegistry();
        new HorizonMetricsBridge(dropwizard, PREFIX).bindTo(micrometer);

        source.inc(3);
        assertThat(micrometer.find("opennms.collectd.persists").functionCounter().count()).isEqualTo(3.0);

        source.inc(5);
        assertThat(micrometer.find("opennms.collectd.persists").functionCounter().count()).isEqualTo(8.0);
    }

    @Test
    void mirrorsAllFiveDropwizardMeterTypes() {
        MetricRegistry dropwizard = new MetricRegistry();
        dropwizard.gauge("kafka.lag", () -> () -> 12L);
        dropwizard.counter("rpc.requests");
        dropwizard.histogram("rpc.payloadBytes");
        dropwizard.meter("rpc.errors");
        dropwizard.timer("rpc.latency");

        SimpleMeterRegistry micrometer = new SimpleMeterRegistry();
        new HorizonMetricsBridge(dropwizard, PREFIX).bindTo(micrometer);

        assertThat(micrometer.find("opennms.kafka.lag").gauge()).isNotNull();
        assertThat(micrometer.find("opennms.rpc.requests").functionCounter()).isNotNull();
        assertThat(micrometer.find("opennms.rpc.payloadBytes.count").functionCounter()).isNotNull();
        assertThat(micrometer.find("opennms.rpc.errors").functionCounter()).isNotNull();
        assertThat(micrometer.find("opennms.rpc.latency").functionTimer()).isNotNull();
    }

    @Test
    void timerMirrorReportsCount() {
        MetricRegistry dropwizard = new MetricRegistry();
        Timer timer = dropwizard.timer("provisiond.scanDuration");
        SimpleMeterRegistry micrometer = new SimpleMeterRegistry();
        new HorizonMetricsBridge(dropwizard, PREFIX).bindTo(micrometer);

        timer.update(java.time.Duration.ofMillis(50));
        timer.update(java.time.Duration.ofMillis(75));

        assertThat(micrometer.find("opennms.provisiond.scanDuration").functionTimer().count()).isEqualTo(2L);
    }

    @Test
    void appliesPrefixToEveryMeter() {
        MetricRegistry dropwizard = new MetricRegistry();
        dropwizard.counter("a");
        dropwizard.counter("b.c");

        SimpleMeterRegistry micrometer = new SimpleMeterRegistry();
        new HorizonMetricsBridge(dropwizard, "minion").bindTo(micrometer);

        assertThat(micrometer.find("minion.a").functionCounter()).isNotNull();
        assertThat(micrometer.find("minion.b.c").functionCounter()).isNotNull();
        assertThat(micrometer.find("a").functionCounter()).isNull();
    }
}
