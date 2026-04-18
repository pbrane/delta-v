/* Copyright (C) 2026 BeaconStrategists, Inc.  AGPL-3.0-or-later */
package org.deltav.prometheus.writer.rw;

import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

@Component
public class RemoteWriteRetryPolicy {
    public enum Action { SUCCESS, RETRY_WITH_BACKOFF, POISON_DLQ, CIRCUIT_OPEN }

    public Action classifyStatus(int status) {
        if (status == 200 || status == 204) return Action.SUCCESS;
        if (status == 429) return Action.RETRY_WITH_BACKOFF;
        if (status >= 500) return Action.RETRY_WITH_BACKOFF;
        if (status == 400 || status == 413) return Action.POISON_DLQ;
        if (status == 401 || status == 403 || status == 404) return Action.CIRCUIT_OPEN;
        if (status >= 400 && status < 500) return Action.POISON_DLQ;
        return Action.POISON_DLQ;
    }

    public Action classifyException(Throwable t) {
        if (t instanceof SocketTimeoutException
                || t instanceof ConnectException
                || t instanceof UnknownHostException) return Action.RETRY_WITH_BACKOFF;
        Throwable cause = t.getCause();
        if (cause != null && cause != t) return classifyException(cause);
        return Action.RETRY_WITH_BACKOFF;
    }
}
