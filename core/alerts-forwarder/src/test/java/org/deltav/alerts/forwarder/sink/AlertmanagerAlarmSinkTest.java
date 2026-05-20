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

import static org.assertj.core.api.Assertions.assertThat;

class AlertmanagerAlarmSinkTest {

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

    private AlertmanagerAlarmSink sink() {
        AlertsForwarderProperties props = new AlertsForwarderProperties();
        props.getAlertmanager().setUrl(server.url("/api/v2/alerts").toString());
        props.getAlertmanager().setResolveTimeoutMs(300000);
        return new AlertmanagerAlarmSink(props);
    }

    @Test
    void postsFiringAlertAsJsonArray() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        ForwardedAlarm alarm = new ForwardedAlarm("rk", ForwardedAlarm.State.FIRING,
                Map.of("node", "Servers:web-01", "severity", "MAJOR"),
                Map.of("description", "Node is down"), 1_700_000_000_000L);

        sink().forward(alarm);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getMethod()).isEqualTo("POST");
        assertThat(req.getPath()).isEqualTo("/api/v2/alerts");
        String body = req.getBody().readUtf8();
        assertThat(body).startsWith("[").endsWith("]");
        assertThat(body).contains("\"node\":\"Servers:web-01\"");
        assertThat(body).contains("\"description\":\"Node is down\"");
        assertThat(body).contains("\"startsAt\"").contains("\"endsAt\"");
    }

    @Test
    void throwsOnNon2xx() {
        server.enqueue(new MockResponse().setResponseCode(503));
        ForwardedAlarm alarm = new ForwardedAlarm("rk", ForwardedAlarm.State.FIRING,
                Map.of("node", "n"), Map.of(), 1L);

        org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> sink().forward(alarm));
    }

    @Test
    void clampsStartsAtWhenInFuture() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        long farFuture = System.currentTimeMillis() + java.time.Duration.ofHours(1).toMillis();
        ForwardedAlarm alarm = new ForwardedAlarm("rk", ForwardedAlarm.State.FIRING,
                Map.of("node", "Servers:web-01"), Map.of(), farFuture);

        sink().forward(alarm);

        RecordedRequest req = server.takeRequest();
        assertThat(req.getMethod()).isEqualTo("POST");
        String body = req.getBody().readUtf8();
        // The annotation must carry the original (unclamped) startsAt.
        assertThat(body).contains("x-deltav-startsAt-original");
        // The startsAt sent to Alertmanager must NOT be the far-future value.
        // The clamped value is endsAt - 1s (now + resolveTimeout - 1s); verify
        // it does not contain the far-future epoch in ISO-8601 form — a rough
        // but sufficient check given the 1-hour gap.
        java.time.Instant farFutureInstant = java.time.Instant.ofEpochMilli(farFuture);
        assertThat(body).doesNotContain("\"startsAt\":\"" + farFutureInstant);
    }
}
