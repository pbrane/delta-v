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
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TwinChannelDispatcherTest {

    /**
     * The Twin response listener must consume horizon's <em>global</em> response
     * topic {@code DeltaV.twin.response}, where broadcast (location-null) updates —
     * including passive-status — are published by horizon's KafkaTwinPublisher.
     * A pattern requiring the {@code .<location>} suffix leaves the consumer with
     * zero partitions, so passive-status Twin updates never bridge to the Minion's
     * gRPC stream and passive outages are never created (issue #284).
     */
    @Test
    void topicPattern_matchesGlobalResponseTopic() {
        Pattern p = Pattern.compile(TwinChannelDispatcher.TWIN_RESPONSE_TOPIC_PATTERN);

        // The global (broadcast / location-null) topic — the one #284 was missing.
        assertTrue(p.matcher("DeltaV.twin.response").matches(),
            "must consume the global DeltaV.twin.response topic (broadcast/location-null updates)");
        // Per-location topics must still match.
        assertTrue(p.matcher("DeltaV.twin.response.Default").matches(),
            "must still consume per-location DeltaV.twin.response.<location> topics");
        assertTrue(p.matcher("DeltaV.twin.response.nl6-lab").matches(),
            "must consume per-location topics with hyphenated locations");
        // Unrelated twin topics must not match.
        assertFalse(p.matcher("DeltaV.twin.request").matches(),
            "must not consume the twin request topic");
    }

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

    /**
     * Horizon's KafkaTwinPublisher routes a broadcast (location-null) publish to
     * the global {@code DeltaV.twin.response} topic with an EMPTY location field.
     * passive-status is always published this way. The dispatcher must fan such a
     * global update out to subscribers at every location — a Minion subscribes at
     * its own location (e.g. {@code Default}), so an exact-location match would
     * silently drop the update and the Minion's PassiveStatusHolder would never
     * learn of the Down (issue #284, secondary cause).
     */
    @Test
    void globalPublish_emptyLocation_broadcastsToLocationSpecificSubscribers() {
        TwinStateCache cache = new TwinStateCache();
        MinionTwinSubscriberRegistry registry = new MinionTwinSubscriberRegistry();
        TwinPatchGenerator patcher = new TwinPatchGenerator();
        TwinChannelDispatcher d = new TwinChannelDispatcher(cache, registry, patcher);

        // A Minion subscribed at location=Default (the only location it knows).
        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        registry.register("passive-status", "Default", obs);

        // Daemon publishes GLOBAL (empty location), as horizon does for passive-status.
        TwinResponseProto global = TwinResponseProto.newBuilder()
            .setConsumerKey("passive-status")
            .setLocation("")
            .setTwinObject(ByteString.copyFromUtf8("{\"AWS\":\"Down\"}"))
            .setVersion(1)
            .setSessionId("s1")
            .build();

        d.handleHorizonUpdate(global);

        // The Default subscriber MUST receive the global update.
        verify(obs).onNext(argThat(u ->
            "passive-status".equals(u.getConsumerKey()) && !u.getIsPatch() && u.getVersion() == 1));
    }

    /**
     * A global (broadcast) update must ALWAYS be sent as a full snapshot, even
     * when a JSON patch would otherwise be generatable. The Minion's horizon
     * {@code AbstractTwinSubscriber} deserializes the wire bytes directly as the
     * target config class — it does not apply RFC 6902 patches — so a patch
     * (a JSON array) fails with MismatchedInputException and the Minion's
     * PassiveStatusHolder is never updated (issue #284, exposed once delivery
     * was fixed). Location-scoped updates keep the patch optimization.
     */
    @Test
    void globalPublish_secondUpdate_staysSnapshotNotPatch() {
        TwinStateCache cache = new TwinStateCache();
        MinionTwinSubscriberRegistry registry = new MinionTwinSubscriberRegistry();
        TwinPatchGenerator patcher = new TwinPatchGenerator();
        TwinChannelDispatcher d = new TwinChannelDispatcher(cache, registry, patcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        registry.register("passive-status", "Default", obs);

        // First global publish (v1) — snapshot; subscriber advances to v1.
        d.handleHorizonUpdate(globalUpdate("{\"AWS\":\"Up\"}", 1, "s1"));
        // Second global publish (v2), same session — a patch WOULD be possible
        // here, but a global update must remain a snapshot.
        d.handleHorizonUpdate(globalUpdate("{\"AWS\":\"Down\"}", 2, "s1"));

        // Both updates delivered as full snapshots (never a patch array).
        verify(obs, times(2)).onNext(argThat(u -> !u.getIsPatch()));
    }

    private static TwinResponseProto globalUpdate(String json, int version, String session) {
        return TwinResponseProto.newBuilder()
            .setConsumerKey("passive-status")
            .setLocation("")
            .setTwinObject(ByteString.copyFromUtf8(json))
            .setVersion(version)
            .setSessionId(session)
            .build();
    }

    /**
     * Horizon's AbstractTwinPublisher sends a full snapshot first, then RFC 6902
     * JSON-Patch deltas with {@code is_patch_object=true}. The gateway must APPLY
     * the inbound patch to the cached full state and broadcast the reconstructed
     * full state — not forward the patch array as if it were a snapshot, which
     * makes the Minion's deserializer fail on an Array value (issue #284, the
     * blocker that surfaced once delivery + global routing were fixed).
     */
    @Test
    void inboundHorizonPatch_appliedToCachedState_broadcastsFullState() {
        TwinStateCache cache = new TwinStateCache();
        MinionTwinSubscriberRegistry registry = new MinionTwinSubscriberRegistry();
        TwinPatchGenerator patcher = new TwinPatchGenerator();
        TwinChannelDispatcher d = new TwinChannelDispatcher(cache, registry, patcher);

        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        registry.register("passive-status", "Default", obs);

        // 1) Full snapshot establishing the base state {}.
        d.handleHorizonUpdate(globalUpdate("{}", 1, "s1"));
        // 2) Horizon patch (is_patch_object=true): add AWS=Down as a JSON-Patch array.
        ByteString patch = patcher.diff(
            ByteString.copyFromUtf8("{}"),
            ByteString.copyFromUtf8("{\"AWS\":\"Down\"}"));
        TwinResponseProto patchUpdate = TwinResponseProto.newBuilder()
            .setConsumerKey("passive-status").setLocation("")
            .setTwinObject(patch)
            .setIsPatchObject(true)
            .setVersion(2).setSessionId("s1").build();
        d.handleHorizonUpdate(patchUpdate);

        // The Minion must receive the reconstructed FULL state {"AWS":"Down"} as a
        // snapshot — never the raw patch array.
        verify(obs).onNext(argThat(u ->
            u.getVersion() == 2
            && !u.getIsPatch()
            && u.getTwinObject().toStringUtf8().contains("AWS")
            && u.getTwinObject().toStringUtf8().contains("Down")
            && !u.getTwinObject().toStringUtf8().contains("\"op\"")));
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
