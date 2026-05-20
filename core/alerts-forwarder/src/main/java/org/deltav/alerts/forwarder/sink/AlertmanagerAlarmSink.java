/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.sink;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.deltav.alerts.forwarder.config.AlertsForwarderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Forwards alarms to Alertmanager's {@code POST /api/v2/alerts}. The request
 * body is a one-element JSON array; a FIRING alert gets {@code endsAt = now +
 * resolveTimeout} so Alertmanager auto-resolves if the forwarder stops
 * refreshing it; a RESOLVED alert gets {@code endsAt = now}.
 *
 * <p>Activated by {@code deltav.alerts-forwarder.alertmanager.enabled} (default
 * true).</p>
 */
@Component
@ConditionalOnProperty(prefix = "deltav.alerts-forwarder.alertmanager",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class AlertmanagerAlarmSink implements AlarmSink {

    private static final Logger LOG = LoggerFactory.getLogger(AlertmanagerAlarmSink.class);

    private final RestClient client;
    private final String url;
    private final long resolveTimeoutMs;

    public AlertmanagerAlarmSink(AlertsForwarderProperties props) {
        this.url = props.getAlertmanager().getUrl();
        this.resolveTimeoutMs = props.getAlertmanager().getResolveTimeoutMs();
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        factory.setReadTimeout((int) Duration.ofSeconds(10).toMillis());
        this.client = RestClient.builder().requestFactory(factory).build();
    }

    @Override
    public void forward(ForwardedAlarm alarm) throws Exception {
        Instant now = Instant.now();
        Instant endsAt = alarm.state() == ForwardedAlarm.State.RESOLVED
                ? now
                : now.plus(Duration.ofMillis(resolveTimeoutMs));

        Instant requestedStartsAt = Instant.ofEpochMilli(alarm.startsAtMs());
        Instant clampedStartsAt = requestedStartsAt.isAfter(endsAt.minusSeconds(1))
                ? endsAt.minusSeconds(1)
                : requestedStartsAt;
        boolean clamped = !requestedStartsAt.equals(clampedStartsAt);
        if (clamped) {
            LOG.warn("Clamped startsAt for alarm reductionKey={} sink={}: requested {} -> {} (would exceed endsAt {})",
                    alarm.reductionKey(), name(), requestedStartsAt, clampedStartsAt, endsAt);
        }

        Map<String, String> annotations = new LinkedHashMap<>(alarm.annotations());
        if (clamped) {
            annotations.put("x-deltav-startsAt-original", requestedStartsAt.toString());
        }

        Map<String, Object> alert = Map.of(
                "labels", alarm.labels(),
                "annotations", annotations,
                "startsAt", clampedStartsAt.toString(),
                "endsAt", endsAt.toString());

        client.post()
                .uri(url)
                .contentType(MediaType.APPLICATION_JSON)
                .body(List.of(alert))
                .retrieve()
                .toBodilessEntity();
    }

    @Override
    public String name() {
        return "alertmanager";
    }
}
