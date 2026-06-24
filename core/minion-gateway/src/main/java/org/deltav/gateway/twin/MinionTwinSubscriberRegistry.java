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

import io.grpc.stub.StreamObserver;
import org.deltav.minion.grpc.v1.TwinUpdate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-(consumer_key, location) registry of currently-subscribed Minion
 * streams plus per-subscriber {@code lastSentVersion}. Different from
 * {@link org.deltav.gateway.rpc.MinionStreamPool} — Twin is broadcast,
 * not load-balanced: one update goes to ALL subscribers in the set.
 *
 * <p>Per Decision 2 sub-decision 2-iii of the v1.2.0-rc2 decisions doc.
 * Subscriptions tracked here are recovered on gateway restart by Minions
 * reconnecting (their existing subscription set is re-sent over the
 * fresh stream); no inter-gateway coordination required.
 */
@Component
public class MinionTwinSubscriberRegistry {

    private final Map<Key, Map<StreamObserver<TwinUpdate>, Subscription>> subs = new ConcurrentHashMap<>();

    public synchronized void register(String consumerKey, String location, StreamObserver<TwinUpdate> observer) {
        Objects.requireNonNull(consumerKey, "consumerKey");
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(observer, "observer");
        subs.computeIfAbsent(new Key(consumerKey, location), k -> new ConcurrentHashMap<>())
            .put(observer, new Subscription(observer, 0));
    }

    public synchronized void unregister(String consumerKey, String location, StreamObserver<TwinUpdate> observer) {
        Map<StreamObserver<TwinUpdate>, Subscription> map = subs.get(new Key(consumerKey, location));
        if (map != null) {
            map.remove(observer);
        }
    }

    public synchronized void unregisterAll(StreamObserver<TwinUpdate> observer) {
        for (Map<StreamObserver<TwinUpdate>, Subscription> map : subs.values()) {
            map.remove(observer);
        }
    }

    public synchronized void updateLastSentVersion(String consumerKey, String location,
                                                    StreamObserver<TwinUpdate> observer, int version) {
        Map<StreamObserver<TwinUpdate>, Subscription> map = subs.get(new Key(consumerKey, location));
        if (map != null) {
            Subscription prior = map.get(observer);
            if (prior != null) {
                map.put(observer, new Subscription(observer, version));
            }
        }
    }

    public Collection<Subscription> subscribers(String consumerKey, String location) {
        Map<StreamObserver<TwinUpdate>, Subscription> map = subs.get(new Key(consumerKey, location));
        return map == null ? Collections.emptyList() : Collections.unmodifiableCollection(map.values());
    }

    /**
     * All subscriptions for {@code consumerKey} across every location, each paired
     * with the location it subscribed at. Used to fan a global (location-null)
     * publish out to location-scoped subscribers — a Minion always subscribes at
     * its own location, so a global update has to reach every location's set.
     */
    public synchronized List<LocatedSubscription> subscribersForAllLocations(String consumerKey) {
        List<LocatedSubscription> out = new ArrayList<>();
        for (Map.Entry<Key, Map<StreamObserver<TwinUpdate>, Subscription>> e : subs.entrySet()) {
            if (e.getKey().consumerKey().equals(consumerKey)) {
                for (Subscription s : e.getValue().values()) {
                    out.add(new LocatedSubscription(e.getKey().location(), s));
                }
            }
        }
        return out;
    }

    public record Key(String consumerKey, String location) {}
    public record Subscription(StreamObserver<TwinUpdate> observer, int lastSentVersion) {}
    public record LocatedSubscription(String location, Subscription subscription) {}
}
