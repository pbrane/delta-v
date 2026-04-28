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
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.opennms.core.ipc.twin.api.LocalTwinSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Minion-side gRPC Twin subscriber. Receives TwinUpdate messages, applies
 * via horizon's LocalTwinSubscriber, and detects version gaps per
 * Decision 2 sub-decision 2-ii (revised) of the v1.2.0-rc2 decisions doc:
 * a non-contiguous version triggers stream reconnect (which yields a fresh
 * full snapshot from the gateway).
 *
 * <p>State per (consumer_key, location): lastSeenVersion + sessionId. When
 * sessionId changes (publisher restart), the next update is treated as a
 * fresh baseline regardless of version (publisher reset its sequence).
 */
public class MinionTwinStreamClient {

    private static final Logger LOG = LoggerFactory.getLogger(MinionTwinStreamClient.class);

    private final LocalTwinSubscriber localSubscriber;
    private final Consumer<String> reconnectTrigger;

    private final Map<Key, State> state = new HashMap<>();

    public MinionTwinStreamClient(LocalTwinSubscriber localSubscriber, Consumer<String> reconnectTrigger) {
        this.localSubscriber = localSubscriber;
        this.reconnectTrigger = reconnectTrigger;
    }

    public StreamObserver<TwinUpdate> streamObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(TwinUpdate u) { handle(u); }
            @Override
            public void onError(Throwable t) {
                LOG.info("Twin stream error: {}", t.toString());
            }
            @Override
            public void onCompleted() {
                LOG.info("Twin stream closed");
            }
        };
    }

    private void handle(TwinUpdate u) {
        Key k = new Key(u.getConsumerKey(), u.getLocation());
        State prior = state.get(k);
        boolean expected = prior == null
            || !prior.sessionId().equals(u.getSessionId())  // publisher restart - any version is fine
            || u.getVersion() == prior.version() + 1;       // contiguous

        if (!expected) {
            String reason = String.format("version-gap on (%s,%s): expected %d, got %d",
                u.getConsumerKey(), u.getLocation(), prior.version() + 1, u.getVersion());
            LOG.warn("{} - triggering reconnect for fresh snapshot", reason);
            reconnectTrigger.accept(reason);
            return;
        }

        org.opennms.core.ipc.twin.api.TwinUpdate horizonUpdate =
            new org.opennms.core.ipc.twin.api.TwinUpdate(u.getConsumerKey(), u.getLocation(), u.getTwinObject().toByteArray());
        horizonUpdate.setPatch(u.getIsPatch());
        horizonUpdate.setVersion(u.getVersion());
        horizonUpdate.setSessionId(u.getSessionId());
        localSubscriber.accept(horizonUpdate);

        state.put(k, new State(u.getVersion(), u.getSessionId()));
    }

    private record Key(String consumerKey, String location) {}
    private record State(int version, String sessionId) {}
}
