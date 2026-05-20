/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.alerts.forwarder.sink;

/**
 * Output port. An implementation forwards a {@link ForwardedAlarm} to one
 * alerting/metrics backend. Implementations are activated by
 * {@code @ConditionalOnProperty} so an operator chooses which ship.
 *
 * <p>Contract: {@link #forward} throws on a transport failure so the caller
 * (the pipeline) can retry and back-pressure the Kafka consumer. It must NOT
 * swallow failures — a silently-dropped alert is worse than a stalled consumer.
 */
public interface AlarmSink {

    /** Forwards one alarm. Throws on transport failure. */
    void forward(ForwardedAlarm alarm) throws Exception;

    /** Short stable name for metrics tags and logs (e.g. "alertmanager"). */
    String name();
}
