/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.sink;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import org.deltav.alerts.forwarder.config.AlertsForwarderProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.xerial.snappy.Snappy;
import prometheus.prompb.Label;
import prometheus.prompb.Sample;
import prometheus.prompb.TimeSeries;
import prometheus.prompb.WriteRequest;

/**
 * Forwards alarms to VictoriaMetrics as a {@code deltav_alarm_state} gauge via
 * Prometheus remote-write: value {@code 1.0} while firing, {@code 0.0} on
 * resolve. The series labels are the {@link ForwardedAlarm} labels (the same
 * low-cardinality set the Alertmanager sink uses).
 *
 * <p>Activated by {@code deltav.alerts-forwarder.victoriametrics.enabled}
 * (default false — Alertmanager is the default backend).</p>
 */
@Component
@ConditionalOnProperty(prefix = "deltav.alerts-forwarder.victoriametrics",
        name = "enabled", havingValue = "true")
public class VictoriaMetricsAlarmSink implements AlarmSink {

    private static final String METRIC_NAME = "deltav_alarm_state";

    private final RestClient client;
    private final String url;

    public VictoriaMetricsAlarmSink(AlertsForwarderProperties props) {
        this.url = props.getVictoriametrics().getUrl();
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        factory.setReadTimeout((int) Duration.ofSeconds(10).toMillis());
        this.client = RestClient.builder().requestFactory(factory).build();
    }

    @Override
    public void forward(ForwardedAlarm alarm) throws Exception {
        double value = alarm.state() == ForwardedAlarm.State.FIRING ? 1.0 : 0.0;

        // TreeMap keeps labels sorted for stable series identity; __name__ sorts first.
        Map<String, String> labels = new TreeMap<>(alarm.labels());
        labels.put("__name__", METRIC_NAME);

        TimeSeries.Builder ts = TimeSeries.newBuilder();
        labels.forEach((k, v) -> ts.addLabels(Label.newBuilder().setName(k).setValue(v).build()));
        ts.addSamples(Sample.newBuilder()
                .setValue(value)
                .setTimestamp(System.currentTimeMillis())
                .build());

        WriteRequest request = WriteRequest.newBuilder().addTimeseries(ts).build();
        byte[] compressed = Snappy.compress(request.toByteArray());

        client.post()
                .uri(url)
                .contentType(MediaType.parseMediaType("application/x-protobuf"))
                .header("Content-Encoding", "snappy")
                .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                .body(compressed)
                .retrieve()
                .toBodilessEntity();
    }

    @Override
    public String name() {
        return "victoriametrics";
    }
}
