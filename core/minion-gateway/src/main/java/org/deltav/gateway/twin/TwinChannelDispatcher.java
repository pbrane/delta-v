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
 * {@code OpenNMS.twin.response.<location>} Kafka topic, translates the
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
        topicPattern = "OpenNMS\\.twin\\.response\\..*",
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
        ByteString newState = msg.getTwinObject();
        int newVersion = msg.getVersion();
        String newSession = msg.getSessionId();

        TwinStateCache.Entry prior = cache.get(key, location);
        cache.put(key, location, newState, newVersion, newSession);

        for (MinionTwinSubscriberRegistry.Subscription sub : registry.subscribers(key, location)) {
            boolean canPatch = prior != null
                && prior.sessionId().equals(newSession)
                && sub.lastSentVersion() == prior.version();
            ByteString outBytes = canPatch ? patcher.diff(prior.state(), newState) : null;

            TwinUpdate.Builder b = TwinUpdate.newBuilder()
                .setConsumerKey(key)
                .setLocation(location)
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
                registry.updateLastSentVersion(key, location, sub.observer(), newVersion);
            } catch (Throwable t) {
                LOG.warn("Failed to send TwinUpdate to subscriber for key={} location={}; unregistering",
                    key, location, t);
                registry.unregister(key, location, sub.observer());
            }
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
