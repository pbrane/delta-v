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
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.opennms.core.ipc.twin.model.TwinResponseProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Dispatcher for the Twin channel. Consumes horizon's
 * {@code DeltaV.twin.response.<location>} Kafka topic, translates the
 * inbound {@link TwinResponseProto} wire format into rc2's {@link TwinUpdate},
 * updates the {@link TwinStateCache}, and broadcasts to all subscribers in
 * {@link MinionTwinSubscriberRegistry} for the matching (consumer_key, location).
 *
 * <p>Per Decision 2 of the v1.2.0-rc2 decisions doc:
 * <ul>
 *   <li>Initial snapshot on subscribe is full state (is_patch=false).</li>
 *   <li>Subsequent broadcasts are RFC 6902 JSON Patch when generatable
 *       (subscriber's lastSentVersion matches prior cache entry, same session_id),
 *       full snapshot otherwise.</li>
 *   <li>Session changes (publisher restart) always trigger a snapshot fallback.</li>
 * </ul>
 *
 * <p>The wire-format translation is the load-bearing piece: horizon publishes
 * {@link TwinResponseProto} bytes on its Kafka topic; rc2 emits {@link TwinUpdate}
 * over gRPC. Any short-circuit that tries to forward horizon's bytes unchanged
 * over gRPC causes silent corruption (PR1 Phase 4 lesson).
 */
@Component
public class TwinChannelDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(TwinChannelDispatcher.class);

    /**
     * Kafka topic pattern for horizon-published Twin updates. Horizon's
     * {@code KafkaTwinPublisher} routes by location: a broadcast (location-null)
     * update goes to the <em>global</em> topic {@code <instance>.twin.response}
     * (e.g. {@code DeltaV.twin.response}), while a location-scoped update goes to
     * {@code <instance>.twin.response.<location>}. This pattern must match BOTH —
     * passive-status (and any other broadcast) updates land on the global topic,
     * so a pattern that requires the {@code .<location>} suffix silently drops
     * them (the consumer is assigned 0 partitions and never bridges to gRPC).
     */
    static final String TWIN_RESPONSE_TOPIC_PATTERN = "DeltaV\\.twin\\.response(\\..*)?";

    private final TwinStateCache cache;
    private final MinionTwinSubscriberRegistry registry;
    private final TwinPatchGenerator patcher;

    public TwinChannelDispatcher(TwinStateCache cache,
                                 MinionTwinSubscriberRegistry registry,
                                 TwinPatchGenerator patcher) {
        this.cache = cache;
        this.registry = registry;
        this.patcher = patcher;
    }

    @KafkaListener(
        topicPattern = TWIN_RESPONSE_TOPIC_PATTERN,
        groupId = "minion-gateway-twin",
        containerFactory = "twinResponseContainerFactory"
    )
    public void onKafkaTwinResponse(ConsumerRecord<String, byte[]> record) {
        try {
            TwinResponseProto horizonMsg = TwinResponseProto.parseFrom(record.value());
            handleHorizonUpdate(horizonMsg);
        } catch (Exception e) {
            LOG.warn("Failed to parse TwinResponseProto from topic={} key={}", record.topic(), record.key(), e);
        }
    }

    /**
     * Process a Twin update from horizon's wire format. Updates the state cache,
     * broadcasts to subscribers as patch (vs their lastSentVersion) when possible,
     * full snapshot otherwise.
     *
     * <p>Snapshot fallback triggers on: session_id change (publisher restart),
     * lastSentVersion not matching prior cache version (subscriber missed an
     * update), no prior cache entry (first publish), or patch generation failure
     * (corrupt JSON).
     */
    void handleHorizonUpdate(TwinResponseProto msg) {
        String key = msg.getConsumerKey();
        String location = msg.getLocation();
        int newVersion = msg.getVersion();
        String newSession = msg.getSessionId();

        TwinStateCache.Entry prior = cache.get(key, location);

        // Horizon's AbstractTwinPublisher sends a full snapshot first, then RFC 6902
        // JSON-Patch deltas (is_patch_object=true) carrying only the change. Recover
        // the full state by applying the patch to our cached state; forwarding the
        // raw patch array as if it were a snapshot makes the Minion's subscriber
        // fail to deserialize it (issue #284).
        ByteString newState;
        if (msg.getIsPatchObject()) {
            if (prior == null) {
                LOG.warn("Inbound Twin patch for key={} location={} with no cached base state; "
                    + "dropping until the publisher sends a full snapshot", key, location);
                return;
            }
            newState = patcher.apply(prior.state(), msg.getTwinObject());
            if (newState == null) {
                LOG.warn("Failed to apply inbound Twin patch for key={} location={}; dropping update", key, location);
                return;
            }
        } else {
            newState = msg.getTwinObject();
        }

        cache.put(key, location, newState, newVersion, newSession);

        // A global (location-null/empty) publish — how horizon broadcasts
        // passive-status — applies to every location, so fan it out to all of a
        // key's subscribers regardless of the location they subscribed at. A
        // location-scoped publish only reaches that location's subscribers.
        if (isGlobal(location)) {
            // Global broadcasts are always sent as full snapshots: the Minion's
            // horizon AbstractTwinSubscriber deserializes the bytes directly as
            // the config class and cannot apply a JSON-Patch array, so a patch
            // would fail to deserialize on the Minion (issue #284). Snapshots are
            // also the safe choice when fanning one update out to many subscribers
            // at independent versions across locations.
            for (MinionTwinSubscriberRegistry.LocatedSubscription ls : registry.subscribersForAllLocations(key)) {
                deliver(key, ls.location(), ls.subscription(), prior, newState, newVersion, newSession, false);
            }
        } else {
            for (MinionTwinSubscriberRegistry.Subscription sub : registry.subscribers(key, location)) {
                deliver(key, location, sub, prior, newState, newVersion, newSession, true);
            }
        }
    }

    private static boolean isGlobal(String location) {
        return location == null || location.isEmpty();
    }

    /**
     * Translate and send one update to a single subscriber, as a JSON patch when
     * the subscriber's lastSentVersion lines up with the prior cached state (same
     * session), full snapshot otherwise. {@code subscriberLocation} is the location
     * the subscriber registered at — which, for a global publish, differs from the
     * (empty) publish location and is where {@code lastSentVersion} is tracked.
     */
    private void deliver(String key, String subscriberLocation,
                         MinionTwinSubscriberRegistry.Subscription sub,
                         TwinStateCache.Entry prior, ByteString newState,
                         int newVersion, String newSession, boolean allowPatch) {
        boolean canPatch = allowPatch
            && prior != null
            && prior.sessionId().equals(newSession)
            && sub.lastSentVersion() == prior.version();
        ByteString outBytes = canPatch ? patcher.diff(prior.state(), newState) : null;

        TwinUpdate.Builder b = TwinUpdate.newBuilder()
            .setConsumerKey(key)
            .setLocation(subscriberLocation)
            .setVersion(newVersion)
            .setSessionId(newSession)
            .setDispatchedAt(now());

        if (outBytes != null) {
            b.setTwinObject(outBytes).setIsPatch(true);
        } else {
            b.setTwinObject(newState).setIsPatch(false);
        }

        try {
            synchronized (sub.observer()) {
                sub.observer().onNext(b.build());
            }
            registry.updateLastSentVersion(key, subscriberLocation, sub.observer(), newVersion);
        } catch (Throwable t) {
            LOG.warn("Failed to send TwinUpdate to subscriber for key={} location={}; unregistering",
                key, subscriberLocation, t);
            registry.unregister(key, subscriberLocation, sub.observer());
        }
    }

    /**
     * Send the initial snapshot to a freshly-subscribed Minion. Called by
     * {@link org.deltav.gateway.grpc.TwinChannelGrpcService} on SUBSCRIBE.
     * If no state is cached yet, no message is sent; the subscriber will
     * receive its first update via the broadcast path when a daemon publishes.
     */
    public void sendInitialSnapshot(String consumerKey, String location, StreamObserver<TwinUpdate> observer) {
        TwinStateCache.Entry e = cache.get(consumerKey, location);
        if (e == null) {
            // Fall back to globally-published state (cached under empty location) —
            // passive-status is broadcast globally, so a location-scoped subscriber
            // still needs it as its initial snapshot.
            e = cache.get(consumerKey, "");
        }
        if (e == null) {
            LOG.info("No cached Twin state for key={} location={}; subscriber will receive on first daemon publish",
                consumerKey, location);
            return;
        }
        TwinUpdate u = TwinUpdate.newBuilder()
            .setConsumerKey(consumerKey)
            .setLocation(location)
            .setVersion(e.version())
            .setSessionId(e.sessionId())
            .setTwinObject(e.state())
            .setIsPatch(false)
            .setDispatchedAt(now())
            .build();
        try {
            synchronized (observer) {
                observer.onNext(u);
            }
            registry.updateLastSentVersion(consumerKey, location, observer, e.version());
        } catch (Throwable t) {
            LOG.warn("Initial snapshot send failed for key={} location={}", consumerKey, location, t);
        }
    }

    private Timestamp now() {
        Instant n = Instant.now();
        return Timestamp.newBuilder().setSeconds(n.getEpochSecond()).setNanos(n.getNano()).build();
    }
}
