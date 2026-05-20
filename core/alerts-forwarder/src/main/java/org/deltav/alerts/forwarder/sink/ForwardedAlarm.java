/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.sink;

import java.util.Map;

/**
 * A transport-agnostic alarm to forward. {@code labels} are low-cardinality
 * dimensions; {@code annotations} are free text. {@code state} drives the
 * lifecycle: FIRING asserts the alert, RESOLVED clears it. {@code startsAtMs}
 * is the alarm's first-event time. A resolve carries the firing alert's stored
 * label set verbatim (see {@code ActiveAlertRegistry}).
 */
public record ForwardedAlarm(
        String reductionKey,
        State state,
        Map<String, String> labels,
        Map<String, String> annotations,
        long startsAtMs) {

    public enum State { FIRING, RESOLVED }
}
