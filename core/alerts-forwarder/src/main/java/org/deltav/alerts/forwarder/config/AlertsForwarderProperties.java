/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds {@code deltav.alerts-forwarder.*}. */
@ConfigurationProperties(prefix = "deltav.alerts-forwarder")
public class AlertsForwarderProperties {

    private String alarmsTopic = "deltav-alarms-state-change";
    private final Filter filter = new Filter();
    private final Alertmanager alertmanager = new Alertmanager();
    private final VictoriaMetrics victoriametrics = new VictoriaMetrics();
    private final Dlq dlq = new Dlq();

    public String getAlarmsTopic() { return alarmsTopic; }
    public void setAlarmsTopic(String v) { this.alarmsTopic = v; }
    public Filter getFilter() { return filter; }
    public Alertmanager getAlertmanager() { return alertmanager; }
    public VictoriaMetrics getVictoriametrics() { return victoriametrics; }
    public Dlq getDlq() { return dlq; }

    public static class Filter {
        private String minSeverity = "WARNING";
        private List<String> ueiDenylist = new ArrayList<>();
        public String getMinSeverity() { return minSeverity; }
        public void setMinSeverity(String v) { this.minSeverity = v; }
        public List<String> getUeiDenylist() { return ueiDenylist; }
        public void setUeiDenylist(List<String> v) { this.ueiDenylist = v; }
    }

    public static class Alertmanager {
        private boolean enabled = true;
        private String url = "http://alertmanager:9093/api/v2/alerts";
        private long resolveTimeoutMs = 300000;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public String getUrl() { return url; }
        public void setUrl(String v) { this.url = v; }
        public long getResolveTimeoutMs() { return resolveTimeoutMs; }
        public void setResolveTimeoutMs(long v) { this.resolveTimeoutMs = v; }
    }

    public static class VictoriaMetrics {
        private boolean enabled = false;
        private String url = "http://victoriametrics:8428/api/v1/write";
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public String getUrl() { return url; }
        public void setUrl(String v) { this.url = v; }
    }

    public static class Dlq {
        private String topic = "deltav-alerts-forwarder-dlq";
        public String getTopic() { return topic; }
        public void setTopic(String v) { this.topic = v; }
    }
}
