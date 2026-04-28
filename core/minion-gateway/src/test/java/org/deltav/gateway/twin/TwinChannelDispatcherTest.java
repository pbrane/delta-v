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
package org.deltav.gateway.twin;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.junit.jupiter.api.Test;
import org.opennms.core.ipc.twin.model.TwinResponseProto;

import java.util.List;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TwinChannelDispatcherTest {

    @Test
    void newSubscriberInitialSnapshot_emitsCachedStateAsSnapshot() {
        TwinStateCache cache = new TwinStateCache();
        cache.put("k", "L", ByteString.copyFromUtf8("{\"x\":1}"), 5, "session-1");

        MinionTwinSubscriberRegistry registry = mock(MinionTwinSubscriberRegistry.class);
        TwinPatchGenerator patcher = new TwinPatchGenerator();
        TwinChannelDispatcher d = new TwinChannelDispatcher(cache, registry, patcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        d.sendInitialSnapshot("k", "L", obs);

        verify(obs).onNext(argThat(u ->
            "k".equals(u.getConsumerKey())
            && !u.getIsPatch()
            && u.getVersion() == 5
            && "session-1".equals(u.getSessionId())));
        verify(registry).updateLastSentVersion("k", "L", obs, 5);
    }

    @Test
    void newSubscriberWithNoCachedState_doesNotEmit() {
        TwinStateCache cache = new TwinStateCache();
        MinionTwinSubscriberRegistry registry = mock(MinionTwinSubscriberRegistry.class);
        TwinPatchGenerator patcher = new TwinPatchGenerator();
        TwinChannelDispatcher d = new TwinChannelDispatcher(cache, registry, patcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        d.sendInitialSnapshot("k", "L", obs);

        // No state in cache yet — wait for first daemon publish.
        verify(obs, org.mockito.Mockito.never()).onNext(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void kafkaUpdate_translatedAndBroadcastAsPatchToExistingSubscribers() {
        TwinStateCache cache = new TwinStateCache();
        cache.put("k", "L", ByteString.copyFromUtf8("{\"x\":1}"), 5, "session-1");

        MinionTwinSubscriberRegistry registry = mock(MinionTwinSubscriberRegistry.class);
        TwinPatchGenerator patcher = new TwinPatchGenerator();
        TwinChannelDispatcher d = new TwinChannelDispatcher(cache, registry, patcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        when(registry.subscribers("k", "L")).thenReturn(List.of(
            new MinionTwinSubscriberRegistry.Subscription(obs, 5)));

        // Daemon publishes: state {x:2}, version 6, same session
        TwinResponseProto horizonMsg = TwinResponseProto.newBuilder()
            .setConsumerKey("k")
            .setLocation("L")
            .setTwinObject(ByteString.copyFromUtf8("{\"x\":2}"))
            .setIsPatchObject(false)  // daemon's own broadcast format may be snapshot or patch; we re-derive
            .setVersion(6)
            .setSessionId("session-1")
            .build();

        d.handleHorizonUpdate(horizonMsg);

        verify(obs).onNext(argThat(u ->
            u.getIsPatch()
            && u.getVersion() == 6
            && u.getTwinObject().toStringUtf8().contains("\"replace\"")));
        verify(registry).updateLastSentVersion("k", "L", obs, 6);
    }

    @Test
    void kafkaUpdate_sessionChange_emitsSnapshotInsteadOfPatch() {
        TwinStateCache cache = new TwinStateCache();
        cache.put("k", "L", ByteString.copyFromUtf8("{\"x\":1}"), 5, "session-A");

        MinionTwinSubscriberRegistry registry = mock(MinionTwinSubscriberRegistry.class);
        TwinPatchGenerator patcher = new TwinPatchGenerator();
        TwinChannelDispatcher d = new TwinChannelDispatcher(cache, registry, patcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        when(registry.subscribers("k", "L")).thenReturn(List.of(
            new MinionTwinSubscriberRegistry.Subscription(obs, 5)));

        // Daemon publisher restarted with new session-id
        TwinResponseProto horizonMsg = TwinResponseProto.newBuilder()
            .setConsumerKey("k").setLocation("L")
            .setTwinObject(ByteString.copyFromUtf8("{\"x\":2}"))
            .setVersion(1).setSessionId("session-B").build();

        d.handleHorizonUpdate(horizonMsg);

        verify(obs).onNext(argThat(u -> !u.getIsPatch() && "session-B".equals(u.getSessionId())));
    }
}
