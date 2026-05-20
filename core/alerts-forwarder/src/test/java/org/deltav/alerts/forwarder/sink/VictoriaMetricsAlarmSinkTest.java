/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.sink;

import java.util.Map;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.deltav.alerts.forwarder.config.AlertsForwarderProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;
import prometheus.prompb.WriteRequest;

import static org.assertj.core.api.Assertions.assertThat;

class VictoriaMetricsAlarmSinkTest {

    private MockWebServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    private VictoriaMetricsAlarmSink sink() {
        AlertsForwarderProperties props = new AlertsForwarderProperties();
        props.getVictoriametrics().setUrl(server.url("/api/v1/write").toString());
        return new VictoriaMetricsAlarmSink(props);
    }

    @Test
    void remoteWritesFiringAsGaugeValueOne() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        ForwardedAlarm alarm = new ForwardedAlarm("rk", ForwardedAlarm.State.FIRING,
                Map.of("node", "Servers:web-01", "severity", "MAJOR"), Map.of(), 1000L);

        sink().forward(alarm);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getHeader("Content-Encoding")).isEqualTo("snappy");
        WriteRequest wr = WriteRequest.parseFrom(Snappy.uncompress(req.getBody().readByteArray()));
        assertThat(wr.getTimeseriesCount()).isEqualTo(1);
        assertThat(wr.getTimeseries(0).getLabelsList())
                .anyMatch(l -> l.getName().equals("__name__") && l.getValue().equals("deltav_alarm_state"));
        assertThat(wr.getTimeseries(0).getSamples(0).getValue()).isEqualTo(1.0);
    }

    @Test
    void remoteWritesResolvedAsGaugeValueZero() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(204));
        ForwardedAlarm alarm = new ForwardedAlarm("rk", ForwardedAlarm.State.RESOLVED,
                Map.of("node", "n"), Map.of(), 1000L);

        sink().forward(alarm);

        WriteRequest wr = WriteRequest.parseFrom(Snappy.uncompress(server.takeRequest().getBody().readByteArray()));
        assertThat(wr.getTimeseries(0).getSamples(0).getValue()).isEqualTo(0.0);
    }
}
