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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.junit.jupiter.api.Test;
import org.opennms.core.ipc.twin.api.LocalTwinSubscriber;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MinionTwinStreamClientTest {

    @Test
    void firstUpdate_isApplied() {
        LocalTwinSubscriber localSubscriber = mock(LocalTwinSubscriber.class);
        @SuppressWarnings("unchecked")
        Consumer<String> reconnectTrigger = mock(Consumer.class);
        MinionTwinStreamClient client = new MinionTwinStreamClient(localSubscriber, reconnectTrigger);

        StreamObserver<TwinUpdate> in = client.streamObserver();
        in.onNext(TwinUpdate.newBuilder()
            .setConsumerKey("k")
            .setLocation("L")
            .setIsPatch(false)
            .setVersion(5)
            .setSessionId("session-1")
            .setTwinObject(ByteString.copyFromUtf8("{\"x\":1}"))
            .build());

        verify(localSubscriber).accept(any(org.opennms.core.ipc.twin.api.TwinUpdate.class));
    }

    @Test
    void contiguousUpdate_isApplied() {
        LocalTwinSubscriber localSubscriber = mock(LocalTwinSubscriber.class);
        @SuppressWarnings("unchecked")
        Consumer<String> reconnectTrigger = mock(Consumer.class);
        MinionTwinStreamClient client = new MinionTwinStreamClient(localSubscriber, reconnectTrigger);

        StreamObserver<TwinUpdate> in = client.streamObserver();
        in.onNext(buildSnapshot("k", "L", "session-1", 5));
        in.onNext(buildPatch("k", "L", "session-1", 6));

        verify(localSubscriber, org.mockito.Mockito.times(2))
            .accept(any(org.opennms.core.ipc.twin.api.TwinUpdate.class));
    }

    @Test
    void nonContiguousVersion_triggersReconnect() {
        LocalTwinSubscriber localSubscriber = mock(LocalTwinSubscriber.class);
        @SuppressWarnings("unchecked")
        Consumer<String> reconnectTrigger = mock(Consumer.class);
        MinionTwinStreamClient client = new MinionTwinStreamClient(localSubscriber, reconnectTrigger);

        StreamObserver<TwinUpdate> in = client.streamObserver();
        in.onNext(buildSnapshot("k", "L", "session-1", 5));
        in.onNext(buildPatch("k", "L", "session-1", 7));  // jumped from 5 to 7 — gap!

        verify(reconnectTrigger).accept("version-gap on (k,L): expected 6, got 7");
    }

    private TwinUpdate buildSnapshot(String key, String loc, String sess, int v) {
        return TwinUpdate.newBuilder()
            .setConsumerKey(key).setLocation(loc).setSessionId(sess).setVersion(v)
            .setIsPatch(false).setTwinObject(ByteString.copyFromUtf8("{\"x\":1}")).build();
    }

    private TwinUpdate buildPatch(String key, String loc, String sess, int v) {
        return TwinUpdate.newBuilder()
            .setConsumerKey(key).setLocation(loc).setSessionId(sess).setVersion(v)
            .setIsPatch(true).setTwinObject(ByteString.copyFromUtf8("[]")).build();
    }
}
