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
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MinionTwinSubscriberRegistryTest {

    @Test
    void noSubscribers_returnsEmpty() {
        MinionTwinSubscriberRegistry r = new MinionTwinSubscriberRegistry();
        assertThat(r.subscribers("k", "L")).isEmpty();
    }

    @Test
    void register_addsSubscriber() {
        MinionTwinSubscriberRegistry r = new MinionTwinSubscriberRegistry();
        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        r.register("k", "L", obs);

        Collection<MinionTwinSubscriberRegistry.Subscription> subs = r.subscribers("k", "L");
        assertThat(subs).hasSize(1);
        assertThat(subs.iterator().next().observer()).isSameAs(obs);
        assertThat(subs.iterator().next().lastSentVersion()).isEqualTo(0);
    }

    @Test
    void updateLastSentVersion_persists() {
        MinionTwinSubscriberRegistry r = new MinionTwinSubscriberRegistry();
        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        r.register("k", "L", obs);
        r.updateLastSentVersion("k", "L", obs, 7);

        assertThat(r.subscribers("k", "L").iterator().next().lastSentVersion()).isEqualTo(7);
    }

    @Test
    void unregister_removesSubscriber() {
        MinionTwinSubscriberRegistry r = new MinionTwinSubscriberRegistry();
        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        r.register("k", "L", obs);
        r.unregister("k", "L", obs);

        assertThat(r.subscribers("k", "L")).isEmpty();
    }

    @Test
    void unregisterAllForStream_removesAcrossKeys() {
        MinionTwinSubscriberRegistry r = new MinionTwinSubscriberRegistry();
        @SuppressWarnings("unchecked")
        StreamObserver<TwinUpdate> obs = mock(StreamObserver.class);
        r.register("k1", "L", obs);
        r.register("k2", "L", obs);
        r.register("k3", "L", obs);

        r.unregisterAll(obs);

        assertThat(r.subscribers("k1", "L")).isEmpty();
        assertThat(r.subscribers("k2", "L")).isEmpty();
        assertThat(r.subscribers("k3", "L")).isEmpty();
    }
}
