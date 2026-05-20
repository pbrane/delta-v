/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.lifecycle;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.deltav.alerts.forwarder.sink.ForwardedAlarm;
import org.springframework.stereotype.Component;

/**
 * In-memory record of currently-firing alerts, keyed by reduction key. Stores
 * the last forwarded firing {@link ForwardedAlarm} so a later resolve reuses
 * the exact same label set — Alertmanager's dedup fingerprint then matches and
 * the firing alert resolves instead of leaking. Rebuilt on every restart by
 * {@code AlarmStateKafkaConsumer} replaying the compacted alarms topic.
 */
@Component
public class ActiveAlertRegistry {

    private final ConcurrentHashMap<String, ForwardedAlarm> active = new ConcurrentHashMap<>();

    public void markFiring(ForwardedAlarm alarm) {
        active.put(alarm.reductionKey(), alarm);
    }

    public boolean isActive(String reductionKey) {
        return active.containsKey(reductionKey);
    }

    /** Non-destructive lookup of the stored firing identity. */
    public Optional<ForwardedAlarm> getFiring(String reductionKey) {
        return Optional.ofNullable(active.get(reductionKey));
    }

    /** Removes and returns the stored firing identity, or empty if not active. */
    public Optional<ForwardedAlarm> remove(String reductionKey) {
        return Optional.ofNullable(active.remove(reductionKey));
    }

    public int size() {
        return active.size();
    }
}
