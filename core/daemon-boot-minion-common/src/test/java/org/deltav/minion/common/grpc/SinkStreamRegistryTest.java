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
package org.deltav.minion.common.grpc;

import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.SyslogMessage;
import org.deltav.minion.grpc.v1.TelemetryDatagram;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SinkStreamRegistryTest {

    @Test
    void firstCall_invokesSupplierAndCachesResult() {
        SinkStreamRegistry registry = new SinkStreamRegistry();
        @SuppressWarnings("unchecked")
        StreamObserver<SyslogMessage> obs = mock(StreamObserver.class);
        AtomicInteger calls = new AtomicInteger();
        Supplier<StreamObserver<SyslogMessage>> supplier = () -> {
            calls.incrementAndGet();
            return obs;
        };

        StreamObserver<SyslogMessage> result1 = registry.getOrOpen("Syslog", supplier);
        StreamObserver<SyslogMessage> result2 = registry.getOrOpen("Syslog", supplier);

        assertThat(result1).isSameAs(obs);
        assertThat(result2).isSameAs(obs);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void differentModuleIds_haveIndependentEntries() {
        SinkStreamRegistry registry = new SinkStreamRegistry();
        @SuppressWarnings("unchecked")
        StreamObserver<SyslogMessage> syslogObs = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<TelemetryDatagram> telemetryObs = mock(StreamObserver.class);

        StreamObserver<SyslogMessage> r1 = registry.getOrOpen("Syslog", () -> syslogObs);
        StreamObserver<TelemetryDatagram> r2 = registry.getOrOpen("Telemetry-IPFIX", () -> telemetryObs);

        assertThat(r1).isSameAs(syslogObs);
        assertThat(r2).isSameAs(telemetryObs);
    }

    @Test
    void reset_dropsEntryAndNextCallOpensFresh() {
        SinkStreamRegistry registry = new SinkStreamRegistry();
        @SuppressWarnings("unchecked")
        StreamObserver<SyslogMessage> obs1 = mock(StreamObserver.class);
        @SuppressWarnings("unchecked")
        StreamObserver<SyslogMessage> obs2 = mock(StreamObserver.class);
        AtomicInteger calls = new AtomicInteger();
        Supplier<StreamObserver<SyslogMessage>> supplier = () -> {
            int n = calls.incrementAndGet();
            return n == 1 ? obs1 : obs2;
        };

        StreamObserver<SyslogMessage> first = registry.getOrOpen("Syslog", supplier);
        registry.reset("Syslog");
        StreamObserver<SyslogMessage> second = registry.getOrOpen("Syslog", supplier);

        assertThat(first).isSameAs(obs1);
        assertThat(second).isSameAs(obs2);
    }
}
